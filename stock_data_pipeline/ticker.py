import datetime


import pandas as pd  # type: ignore


from .definitions import SQLOperation
from .functions import make_ticker_sql_compatible, make_ticker_yfinance_compatible
from .postgresql_connection import PostgreSQLConnection


class Ticker:

    def __init__(self, ticker: str, postgresql_connection: PostgreSQLConnection):
        self.ticker_symbol = make_ticker_sql_compatible(ticker)
        self.yfinance_ticker = make_ticker_yfinance_compatible(ticker)
        self.table_name = f"{self.ticker_symbol}_stock_history"
        self.price_column_name = f"{self.ticker_symbol}_price"
        self.postgresql_connection = postgresql_connection
        self.stock_history = pd.DataFrame()

        query = f"CREATE TABLE IF NOT EXISTS {self.table_name} (date DATE PRIMARY KEY,open NUMERIC(10, 2),high NUMERIC(10, 2),low NUMERIC(10, 2),close NUMERIC(10, 2),volume BIGINT)"
        self.postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)

    def get_stock_history_latest_date(self) -> datetime.datetime | None:
        """Get most recent date from stock history. If no stock history, return None."""

        query = f"SELECT MAX(DATE) FROM {self.table_name}"
        cursor = self.postgresql_connection.execute_query(
            query, operation=SQLOperation.EXECUTE
        )
        return cursor.fetchone()[0]
