import datetime
from pathlib import Path
import re
from typing import List


from bs4 import BeautifulSoup
import pandas as pd  # type:ignore


from .definitions import (
    SECTOR_SHARES_OUTSTANDING,
    STOCK_WEIGHT_DIRECTORY,
    DataTypes,
    SQLOperation,
    TickerColumnType,
)
from .functions import (
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
        self.url_xlsx = f"https://www.ssga.com/us/en/institutional/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{self.sector_symbol}.xlsx"
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
        missing_columns = [f"{new_ticker}_{column_type.value}" for new_ticker in self.new_tickers]
        for missing_column in missing_columns:
            query = f"ALTER TABLE {sql_table_name} ADD COLUMN {missing_column} {data_type_string} NULL"
            postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)

    def add_ticker(self, ticker_object: Ticker):
        if ticker_object.ticker_symbol not in self.tickers:
            self.tickers.append(ticker_object)
            self.sector_shares_data_types.update({ticker_object.ticker_symbol: DataTypes.BIGINT})

    def calculate_sector_price(self):
        drop_column_query = f"ALTER TABLE {self.sector_history_table_name} DROP COLUMN IF EXISTS {self.sector_calculated_price_column_name}"
        self.postgresql_connection.execute_query(drop_column_query, SQLOperation.COMMIT)

        add_column_query = f"ALTER TABLE {self.sector_history_table_name} ADD COLUMN {self.sector_calculated_price_column_name} NUMERIC(10,2)"
        self.postgresql_connection.execute_query(add_column_query, SQLOperation.COMMIT)

        update_query = f"UPDATE {self.sector_history_table_name}"
        set_query = f"SET {self.sector_calculated_price_column_name} = "
        calculation_queries = []
        for ticker in self.tickers:
            calculation_queries.append(f"{self.sector_history_table_name}.{ticker.price_column_name} * {self.sector_shares_table_name}.{ticker.shares_column_name}")
        calculation_query = f"""{" ( " + " + ".join(calculation_queries) + " ) "} / {SECTOR_SHARES_OUTSTANDING}.{self.sector_symbol}"""
        from_query = f"FROM {SECTOR_SHARES_OUTSTANDING}"
        join_query = f"JOIN {self.sector_shares_table_name} on {self.sector_shares_table_name}.date = {SECTOR_SHARES_OUTSTANDING}.date"
        where_query = f"WHERE {self.sector_shares_table_name}.date = {self.sector_history_table_name}.date"

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

    def create_sector_history_table(self):
        self.sector_history_df = get_s3_table(
            self.s3_connection,
            s3_file_name=self.sector_history_s3_file_name,
            download_file_path=self.sector_history_download_file_path,
        )
        self.add_missing_columns(
            column_type=TickerColumnType.PRICE,
            sql_table_name=self.sector_history_table_name,
            data_type_string=DataTypes.INT,
            postgresql_connection=self.postgresql_connection,
        )
        first_ticker = self.tickers[0]
        first_ticker_table_name = first_ticker.table_name
        first_ticker_price_column = first_ticker.price_column_name
        table_name_query = f"CREATE TABLE IF NOT EXISTS {self.sector_history_table_name} as"
        select_query = f" SELECT {first_ticker_table_name}.date as date"
        column_query = f", {first_ticker_table_name}.close as {first_ticker_price_column}"
        from_query = f" FROM {first_ticker_table_name}"
        join_query = ""
        for ticker in self.tickers[1:]:
            ticker_table_name = ticker.table_name
            column_query += f", {ticker_table_name}.close as {ticker.price_column_name}"
            join_query += f" JOIN {ticker_table_name} ON {first_ticker_table_name}.date = {ticker_table_name}.date"
        order_by_query = f" ORDER BY {first_ticker_table_name}.date ASC"
        query = table_name_query + select_query + column_query + from_query + join_query + order_by_query
        self.postgresql_connection.execute_query(
            f"DROP TABLE IF EXISTS {self.sector_history_table_name}",
            operation=SQLOperation.COMMIT,
        )  # TODO: remove this operation to append data to existing table
        self.postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)

        self.calculate_sector_price()
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
