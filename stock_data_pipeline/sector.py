import datetime
from pathlib import Path
import re
from typing import List


from bs4 import BeautifulSoup
import pandas as pd  # type:ignore
import sqlalchemy

from .definitions import (
    SECTOR_SHARES_OUTSTANDING,
    STOCK_WEIGHT_DIRECTORY,
    DataTypes,
    SQLOperation,
    TickerColumnType,
)
from .functions import (
    convert_sql_data_type_into_string,
    get_latest_date,
    get_s3_table,
    make_ticker_sql_compatible,
)
from stock_data_pipeline import PostgreSQLConnection, S3Connection
from .ticker import Ticker


class Sector:
    def __init__(
        self,
        sector: str,
        postgresql_connection: PostgreSQLConnection,
        s3_connection: S3Connection,
        sector_shares_directory: Path,
    ):
        self.sector_symbol = make_ticker_sql_compatible(sector)
        self.postgresql_connection = postgresql_connection
        self.s3_connection = s3_connection
        self.sector_shares_directory = sector_shares_directory
        self.sector_history_table_name = f"{self.sector_symbol}_sector_history"
        self.sector_shares_table_name = f"{self.sector_symbol}_shares"
        self.sector_history_s3_file_name = f"{self.sector_history_table_name}.csv"
        self.sector_shares_s3_file_name = f"{self.sector_shares_table_name}.csv"
        self.sector_history_download_file_path = Path(self.sector_shares_directory, self.sector_history_s3_file_name)
        self.sector_shares_download_file_path = Path(self.sector_shares_directory, self.sector_shares_s3_file_name)
        self.sector_history_df: pd.DataFrame = pd.DataFrame()
        self.sector_shares_df: pd.DataFrame = pd.DataFrame()
        self.new_tickers: List[str] = []
        self.old_tickers: List[str] = []
        self.sector_calculated_price_column_name = f"{self.sector_symbol}_calculated_price"
        self.shares_outstanding: None | int = None
        self.url_shares_outstanding = f"https://www.ssga.com/us/en/institutional/etfs/the-materials-select-sector-spdr-fund-{self.sector_symbol}"
        self.url_xlsx = (
            f"https://www.ssga.com/us/en/institutional/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{self.sector_symbol}.xlsx"
        )
        self.portfolio_holdings_file_path: Path = Path(
            STOCK_WEIGHT_DIRECTORY,
            f"holdings-daily-us-en-{self.sector_symbol}.xlsx",
        )
        self.tickers: List[Ticker] = []
        self.shares_outstanding: None | int = None
        self.accept_xpath = """//*[@id="js-ssmp-clrButtonLabel"]"""
        self.shares_outstanding_xpath = "//dt[text()='Shares Outstanding']/following-sibling::dd"
        self.portfolio_csv_link_text = "Daily"
        self.portfolio_csv_link_xpath = """//*[@id="holdings"]/div/div[1]/section/div/div[2]/div[1]/div[2]/a"""
        self.sector_shares_data_types = {"date": DataTypes.DATE}

    def add_missing_columns(
        self,
        column_type: TickerColumnType,
        sql_table_name: str,
        data_type_string: str,
        postgresql_connection: PostgreSQLConnection,
    ):
        # missing_columns = [f"{new_ticker}_{column_type.value}" for new_ticker in self.new_tickers]
        for missing_column in self.new_tickers:
            if column_type.value not in missing_column:
                missing_column += f"_{column_type.value}"
            query = f"ALTER TABLE {sql_table_name} ADD COLUMN IF NOT EXISTS {missing_column} {data_type_string} NULL"
            postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)

    def add_ticker(self, ticker_object: Ticker):
        if ticker_object.ticker_symbol not in self.tickers:
            self.tickers.append(ticker_object)
            self.sector_shares_data_types.update({ticker_object.ticker_symbol: DataTypes.BIGINT})

    def calculate_sector_price(self):
        update_query = f"UPDATE {self.sector_history_table_name}"
        set_query = f"SET {self.sector_calculated_price_column_name} = "
        calculation_queries = []
        for ticker in self.tickers:
            calculation_queries.append(
                f"{self.sector_history_table_name}.{ticker.price_column_name} * {self.sector_shares_table_name}.{ticker.shares_column_name}"
            )
        calculation_query = f"""{" ( " + " + ".join(calculation_queries) + " ) "} / {SECTOR_SHARES_OUTSTANDING}.{self.sector_symbol}"""
        from_query = f"FROM {SECTOR_SHARES_OUTSTANDING}"
        join_query = f"JOIN {self.sector_shares_table_name} on {self.sector_shares_table_name}.date = {SECTOR_SHARES_OUTSTANDING}.date"
        where_query = f"WHERE {self.sector_shares_table_name}.date = {self.sector_history_table_name}.date AND {self.sector_history_table_name}.{self.sector_calculated_price_column_name} IS NULL"

        query = " ".join(
            [
                update_query,
                set_query,
                calculation_query,
                from_query,
                join_query,
                where_query,
            ]
        )
        self.postgresql_connection.execute_query(query, SQLOperation.COMMIT)

    def create_sector_history_table(self, todays_date):
        self.sector_history_df = get_s3_table(
            self.s3_connection,
            s3_file_name=self.sector_history_s3_file_name,
            download_file_path=self.sector_history_download_file_path,
        )
        self.sector_history_df.index.name = "date"
        for old_ticker in self.old_tickers:
            old_ticker_price = f"{old_ticker}_price"
            if old_ticker_price in self.sector_history_df.columns:
                self.sector_history_df.drop(labels=old_ticker_price, axis=1, inplace=True)
        sector_history_dtypes = {"date": sqlalchemy.DATE}
        sector_history_dtypes_strings = {
            "date": DataTypes.DATE,
        }
        sector_history_dtypes.update({column: sqlalchemy.types.Numeric(10, 2) for column in self.sector_history_df.columns})
        sector_history_dtypes_strings.update({column: DataTypes.NUMERIC_10_2 for column in self.sector_history_df.columns})
        sector_history_dtypes_strings = convert_sql_data_type_into_string(sector_history_dtypes_strings)
        query = f"DROP TABLE IF EXISTS {make_ticker_sql_compatible(self.sector_history_table_name)}"
        self.postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)
        query = f"CREATE TABLE IF NOT EXISTS {make_ticker_sql_compatible(self.sector_history_table_name)} ({sector_history_dtypes_strings})"
        self.postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)

        self.sector_history_df.loc[todays_date, :] = None
        for ticker in self.tickers:
            self.sector_history_df.loc[todays_date, f"{ticker.ticker_symbol}_price"] = ticker.price  # TODO: Convert numpy float to float
        self.sector_history_df.to_sql(
            make_ticker_sql_compatible(self.sector_history_table_name),
            con=self.postgresql_connection.engine,
            if_exists="replace",
            index=True,
            index_label="date",
            dtype=sector_history_dtypes,
        )

        self.calculate_sector_price()
        self.sector_history_df = pd.read_sql(self.sector_history_table_name, con=self.postgresql_connection.engine).set_index("date")
        self.s3_connection.upload_sql_table(
            self.sector_history_table_name,
            postgresql_connection=self.postgresql_connection,
        )

    def create_sector_shares_dataframe(self, todays_date: datetime.datetime) -> pd.DataFrame:
        df_sector_shares = pd.read_excel(self.portfolio_holdings_file_path, skiprows=4)[["Ticker", "Weight", "Shares Held"]]
        df_sector_shares.columns = [column.lower().replace(" ", "_") for column in df_sector_shares.columns]
        df_sector_shares = df_sector_shares[df_sector_shares["ticker"] != "-"]  # TODO: Add note as to why this is removed
        df_sector_shares = df_sector_shares[df_sector_shares["ticker"].notna()]  # TODO: Add note as to why this is removed
        df_sector_shares = df_sector_shares[~df_sector_shares["ticker"].str.contains("5")]
        df_sector_shares["ticker"] = [make_ticker_sql_compatible(ticker) for ticker in df_sector_shares["ticker"]]
        df_sector_shares = df_sector_shares.sort_values(by="ticker")

        df_sector_shares["weight"] = df_sector_shares["weight"] / 100
        df_sector_shares["date"] = todays_date.strftime("%Y-%m-%d")
        df_sector_shares = pd.pivot(df_sector_shares, index="date", columns="ticker", values="shares_held")
        return df_sector_shares

    def get_new_tickers(self, original_tickers: List[str], latest_tickers: List[str]):
        self.new_tickers = [column for column in latest_tickers if column not in original_tickers]  # TODO: add missing columns to sql_table

    def get_s3_table(self):
        self.s3_connection.download_file(
            self.sector_shares_s3_file_name,
            download_file_path=self.sector_shares_download_file_path,
        )
        if self.sector_shares_download_file_path.exists():
            self.sector_shares_df = pd.read_csv(self.sector_shares_download_file_path)
            self.sector_shares_df.index = pd.to_datetime(self.sector_shares_df["date"]).dt.strftime("%Y-%m-%d")
            self.sector_shares_df.index.name = None
            self.sector_shares_df.drop(labels="date", inplace=True, axis=1)

    def get_s3_table_latest_date(self) -> pd.DatetimeIndex | None:
        return get_latest_date(self.sector_shares_df, date_format="%Y-%m-%d")

    def parse_shares_outstanding(self, html: str):
        soup = BeautifulSoup(html, "html.parser")

        # Find the <td> that contains 'Shares Outstanding'
        label_td = soup.find("td", string=re.compile(r"\bShares Outstanding\b", re.I))

        # If found, get the next sibling <td> which contains the number
        if label_td:
            data_td = label_td.find_next_sibling("td", class_="data")
            if data_td:
                match = re.search(r"([\d,.]+)\s*([MB])", data_td.text.strip())
                if match:
                    number = match.group(1)
                    suffix = match.group(2)
                    return f"{number} {suffix}"
                else:
                    print("Pattern not found.")
            else:
                print("Data cell not found.")
        else:
            print("Label cell not found.")
