import datetime
from pathlib import Path
from typing import List


import pandas as pd  # type:ignore


from .chrome_driver import ChromeDriver
from .definitions import SECTOR_SHARES_OUTSTANDING, DataTypes, SQLOperation
from .functions import (
    make_ticker_sql_compatible,
)
from .postgresql_connection import PostgreSQLConnection
from .ticker import Ticker


class Sector:

    download_file_directory_path: Path

    def __init__(
        self,
        sector: str,
        chrome_driver: ChromeDriver,
        postgresql_connection: PostgreSQLConnection,
    ):
        self.download_file_directory_path = chrome_driver.download_file_directory_path
        self.sector_symbol = make_ticker_sql_compatible(sector)
        self.postgresql_connection = postgresql_connection
        self.sector_history_table_name = f"{self.sector_symbol}_sector_history"
        self.sector_shares_table_name = f"{self.sector_symbol}_shares"
        self.sector_calculated_price_column_name = (
            f"{self.sector_symbol}_calculated_price"
        )
        self.shares_outstanding: None | int = None
        self.url = f"https://www.sectorspdrs.com/mainfund/{self.sector_symbol}"
        self.index_holdings_file_path: Path = Path(
            self.download_file_directory_path,
            f"index-holdings-{self.sector_symbol}.csv",
        )
        self.portfolio_holdings_file_path: Path = Path(
            self.download_file_directory_path,
            f"portfolio-holdings-{self.sector_symbol}.csv",
        )
        self.tickers: List[Ticker] = []
        self.shares_outstanding: None | int = None
        self.shares_outstanding_xpath = (
            "//dt[text()='Shares Outstanding']/following-sibling::dd"
        )
        self.index_csv_xpath = "(//span[contains(text(), 'Download a Spreadsheet')]/following-sibling::button[contains(text(), 'CSV File')])[1]"
        self.portfolio_tab_xpath = "//a[contains(text(), 'Portfolio Holdings')]"
        self.portfolio_csv_xpath = "(//span[contains(text(), 'Download a Spreadsheet')]/following-sibling::button[contains(text(), 'CSV File')])[2]"
        self.sector_shares_data_types = {"Date": DataTypes.DATE}

    def add_ticker(self, ticker_object: Ticker):
        if ticker_object.ticker_symbol not in self.tickers:
            self.tickers.append(ticker_object)
            self.sector_shares_data_types.update(
                {ticker_object.ticker_symbol: DataTypes.BIGINT}
            )

    def calculate_sector_price(self):
        drop_column_query = f"ALTER TABLE {self.sector_history_table_name} DROP COLUMN IF EXISTS {self.sector_calculated_price_column_name}"
        self.postgresql_connection.execute_query(drop_column_query, SQLOperation.COMMIT)

        add_column_query = f"ALTER TABLE {self.sector_history_table_name} ADD COLUMN {self.sector_calculated_price_column_name} NUMERIC(10,2)"
        self.postgresql_connection.execute_query(add_column_query, SQLOperation.COMMIT)

        update_query = f"UPDATE {self.sector_history_table_name}"
        set_query = f"SET {self.sector_calculated_price_column_name} = "
        calculation_queries = []
        for ticker in self.tickers:
            calculation_queries.append(
                f"{self.sector_history_table_name}.{ticker.price_column_name} * {self.sector_shares_table_name}.{ticker.ticker_symbol}"
            )
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

        # TODO: create multiple private functions to make code more readable
        first_ticker = self.tickers[0]
        first_ticker_table_name = first_ticker.table_name
        first_ticker_price_column = first_ticker.price_column_name
        table_name_query = f"CREATE TABLE IF NOT EXISTS {self.sector_history_table_name} as"  # TODO: revert operation to 'IF NOT EXISTS'
        select_query = f" SELECT {first_ticker_table_name}.date as date"
        column_query = (
            f", {first_ticker_table_name}.close as {first_ticker_price_column}"
        )
        from_query = f" FROM {first_ticker_table_name}"
        join_query = ""
        order_by_query = f" ORDER BY {first_ticker_table_name}.date ASC"
        for ticker in self.tickers[1:]:
            ticker_table_name = ticker.table_name
            column_query += f", {ticker_table_name}.close as {ticker.price_column_name}"
            join_query += f" JOIN {ticker_table_name} ON {first_ticker_table_name}.date = {ticker_table_name}.date"
        query = (
            table_name_query
            + select_query
            + column_query
            + from_query
            + join_query
            + order_by_query
        )
        self.postgresql_connection.execute_query(
            f"DROP TABLE IF EXISTS {self.sector_history_table_name}",
            operation=SQLOperation.COMMIT,
        )  # TODO: remove this operation to append data to existing table
        self.postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)

        self.calculate_sector_price()

    def create_sector_shares_dataframe(
        self, todays_date: datetime.datetime
    ) -> pd.DataFrame:

        df_sector_shares = pd.read_csv(self.portfolio_holdings_file_path, header=1)[
            ["Symbol", "Weight", "Shares Held"]
        ]
        df_sector_shares.columns = [
            column.lower().replace(" ", "_") for column in df_sector_shares.columns
        ]
        df_sector_shares = df_sector_shares[
            df_sector_shares["symbol"].notna()
        ]  # TODO: Add note as to why this is removed
        df_sector_shares = df_sector_shares[
            ~df_sector_shares["symbol"].str.contains("25")
        ]  # TODO: Add note as to why this is removed
        df_sector_shares["symbol"] = [
            make_ticker_sql_compatible(symbol) for symbol in df_sector_shares["symbol"]
        ]
        df_sector_shares = df_sector_shares.sort_values(by="symbol")

        df_sector_shares["weight"] = (
            df_sector_shares["weight"].str.rstrip("%").astype(float) / 100
        )
        df_sector_shares["shares_held"] = (
            df_sector_shares["shares_held"].str.replace(",", "").astype(int)
        )
        df_sector_shares["date"] = todays_date.strftime("%Y-%m-%d")
        df_sector_shares = pd.pivot(
            df_sector_shares, index="date", columns="symbol", values="shares_held"
        )
        return df_sector_shares
