"""Main script to run the stock data pipeline.
Download stock data from Yahoo Finance, transform data in SQL, and load data into AWS.
"""

from pathlib import Path
from stock_data_pipeline.load_yfinance_data import CollectDailyData
import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

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

float_columns = ["Open", "High", "Low", "Close"]


# Create list of stocks
tickers = []
config_directory = Path("config")
stocks_file_path = Path(config_directory, "stocks.txt")
with open(stocks_file_path, "r", encoding="utf-8") as file:
    for ticker in file:
        tickers.append(ticker.rstrip("\n").lower())


def connect_postgresql_local_server():
    connection = psycopg2.connect(**DB_PARAMS)
    return connection, connection.cursor()


def execute_query(cursor, query, values=None):
    if values:
        cursor.execute(query, values)  # Use values to parameterize the query
    else:
        cursor.execute(query)
    return cursor


def add_data(cursor, engine, ticker: str, stock_history: pd.DataFrame):
    table_query = f"SELECT * FROM {ticker} LIMIT 1"
    cursor = execute_query(cursor, table_query)
    data = cursor.fetchone()
    if data is None:
        # TODO: need to append table, since empty table exists.
        # TODO: need to align schema in pandas to schema in SQL
        stock_history.to_sql(
            ticker,
            con=engine,
            if_exists="replace",
            index=True,
            index_label="date",
            dtype=STOCK_HISTORY_DTYPES,
        )
    else:
        # TODO: need to edit else statement to insert data when data exists in table
        query = f"""INSERT INTO {ticker} (date, open, high, low, close, volume) 
            VALUES (%s, %s, %s, %s, %s, %s)"""
        cursor = execute_query(cursor, query, values=data)


def create_stock_data_table(connection, cursor, ticker: str):
    """Create a table for stock if table does not exist."""
    query = f"CREATE TABLE IF NOT EXISTS {ticker} (date DATE PRIMARY KEY,open NUMERIC(10, 2),high NUMERIC(10, 2),low NUMERIC(10, 2),close NUMERIC(10, 2),volume BIGINT)"
    cursor = execute_query(cursor, query)
    connection.commit()


def get_latest_date(cursor, ticker: str):
    query = f"SELECT MAX(DATE) FROM {ticker}"
    cursor = execute_query(cursor, query)
    return cursor.fetchone()[0]


def transform_stock_data(connection, ticker):
    pass


connection, cursor = connect_postgresql_local_server()
engine = create_engine(SQLALCHEMY_CONNECTION_STRING)

stock_data_directory = Path("data")
for ticker in tickers:
    create_stock_data_table(
        connection, cursor, ticker
    )  # Create blank table if table for stock does not exist
    latest_date = get_latest_date(cursor, ticker)
    collect_stock_data = CollectDailyData(ticker, latest_date=latest_date)
    stock_history = collect_stock_data.get_ticker_history()
    stock_history.columns = [column.lower() for column in stock_history.columns]
    stock_history.index.name = stock_history.index.name.lower()
    add_data(cursor, engine, ticker, stock_history)
transform_stock_data(cursor, ticker)
