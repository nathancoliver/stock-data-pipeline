"""Main script to run the stock data pipeline.
Download stock data from Yahoo Finance, transform data in SQL, and load data into AWS.
"""

from pathlib import Path
from stock_data_pipeline.load_yfinance_data import CollectDailyData
import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import datetime

HOST = "localhost"
PORT = 5432
DATABASE = "stock_history"
USER = "postgres"
PASSWORD = "l"

DB_PARAMS = {
    "host": HOST,
    "port": PORT,
    "dbname": DATABASE,
    "user": USER,
    "password": PASSWORD,
}
SQLALCHEMY_CONNECTION_STRING = (
    f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
)
STOCK_HISTORY_DTYPES = {
    "Date": sqlalchemy.DATE,
    "Open": sqlalchemy.types.Numeric(10, 2),
    "High": sqlalchemy.types.Numeric(10, 2),
    "Low": sqlalchemy.types.Numeric(10, 2),
    "Close": sqlalchemy.types.Numeric(10, 2),
    "Volume": sqlalchemy.types.BigInteger,
}

# Create list of stocks
tickers = []
config_directory = Path("config")
stocks_file_path = Path(config_directory, "stocks.txt")
with open(stocks_file_path, "r", encoding="utf-8") as file:
    for ticker in file:
        tickers.append(ticker.rstrip("\n").lower())


def connect_postgresql_local_server():
    """Create connection to local postgreSQL server."""

    connection = psycopg2.connect(**DB_PARAMS)
    return connection, connection.cursor()


def execute_query(cursor, query, values=None):
    """Execute postgreSQL query."""

    if values:
        cursor.execute(query, values)  # Use values to parameterize the query
    else:
        cursor.execute(query)
    return cursor


def add_data(engine, ticker: str, stock_history: pd.DataFrame):
    """Append stock history data to existing stock history table."""

    stock_history.to_sql(
        ticker,
        con=engine,
        if_exists="append",
        index=True,
        index_label="date",
        dtype=STOCK_HISTORY_DTYPES,
    )


def create_stock_data_table(connection, cursor, ticker: str):
    """Create a stock history table if table does not exist."""

    query = f"CREATE TABLE IF NOT EXISTS {ticker} (date DATE PRIMARY KEY,open NUMERIC(10, 2),high NUMERIC(10, 2),low NUMERIC(10, 2),close NUMERIC(10, 2),volume BIGINT)"
    cursor = execute_query(cursor, query)
    connection.commit()


def get_latest_date(cursor, ticker: str) -> datetime.date | None:
    """Get latest date from stock history. If no stock histroy, return None."""

    query = f"SELECT MAX(DATE) FROM {ticker}"
    cursor = execute_query(cursor, query)
    return cursor.fetchone()[0]


def check_table_append_compatibility(
    latest_date_datetime: datetime.date | None, stock_history: pd.DataFrame
) -> pd.DataFrame:
    """Filter dataframe to not include dates already in postgreSQL stock history table."""

    if latest_date_datetime is not None:
        stock_history_latest_date = stock_history.index[
            -1
        ]  # Get latest date in pandas stock history table.
        latest_date_pandas_datetime = pd.to_datetime(
            latest_date_datetime
        )  # Convert latest date to pandas datetime.
        if latest_date_pandas_datetime >= stock_history_latest_date:
            stock_history = stock_history[
                stock_history.index > latest_date_pandas_datetime
            ]  # Filter pandas stock history to not include any dates already in postgreSQL table.
    return stock_history


def transform_stock_data(connection, cursor, tickers):
    table_name = "_mag_7"
    first_ticker = tickers[0]
    filter_date = "2025-01-01"
    table_name_query = f"CREATE TABLE {table_name} as"
    select_query = f" SELECT {first_ticker}.date as date"
    column_query = f", {first_ticker}.close as {first_ticker}_close"
    from_query = f" FROM {first_ticker}"
    join_query = ""
    where_query = f" WHERE {first_ticker}.date >= '{filter_date}' order by {first_ticker}.date ASC"
    for ticker in tickers[1:]:
        column_query += f", {ticker}.close as {ticker}_close"
        join_query += f" JOIN {ticker} ON {first_ticker}.date = {ticker}.date"

    query = (
        table_name_query
        + select_query
        + column_query
        + from_query
        + join_query
        + where_query
    )
    cursor = execute_query(cursor, query)
    connection.commit()


connection, cursor = (
    connect_postgresql_local_server()
)  # Get postgreSQL connection and cursor.
engine = create_engine(SQLALCHEMY_CONNECTION_STRING)  # Get SQLAlchemy engine.

# Create or update stock history for each ticker.
for ticker in tickers:
    create_stock_data_table(
        connection, cursor, ticker
    )  # Create blank table if table for stock does not exist.
    latest_date = get_latest_date(
        cursor, ticker
    )  # Get latest date of stock history table.
    collect_stock_data = CollectDailyData(
        ticker, latest_date=latest_date
    )  # initialize CollectDailyData class.
    stock_history = (
        collect_stock_data.get_ticker_history()
    )  # retrieve stock history pd.DataFrame.
    stock_history.columns = [
        column.lower() for column in stock_history.columns
    ]  # Set column names to all lower-case letters.
    stock_history.index.name = (
        stock_history.index.name.lower()
    )  # Set date index to lower-case letters.
    stock_history = check_table_append_compatibility(
        latest_date, stock_history
    )  # Filter stock history to ensure no overlapping dates in postgreSQL table.
    if not stock_history.empty:  # Skip add_data if stock history table is empty.
        add_data(engine, ticker, stock_history)  # Append data to stock history table.
transform_stock_data(connection, cursor, tickers)
