"""Main script to run the stock data pipeline.
Download stock data from Yahoo Finance, transform data in SQL, and load data into AWS.
"""

from pathlib import Path
from stock_data_pipeline.load_yfinance_data import CollectDailyData
import psycopg2

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

# Create list of stocks
tickers = []
config_directory = Path("config")
stocks_file_path = Path(config_directory, "stocks.txt")
with open(stocks_file_path, "r", encoding="utf-8") as file:
    for ticker in file:
        tickers.append(ticker.rstrip("\n"))


def connect_postgresql_local_server():
    connection = psycopg2.connect(**DB_PARAMS)
    return connection, connection.cursor()


def execute_query(cursor, query, values=None):
    if values:
        cursor.execute(query, values)  # Use values to parameterize the query
    else:
        cursor.execute(query)
    return cursor


def create_stock_data_table(connection, cursor, ticker: str):
    """Craete a table for stock if table does not exist."""
    query = f"CREATE TABLE IF NOT EXISTS {ticker} (date DATE PRIMARY KEY,open DECIMAL,high DECIMAL,low DECIMAL,close DECIMAL,volume INT)"
    cursor = execute_query(cursor, query)
    connection.commit()
    cursor.execute(query)


def transform_stock_data(connection, ticker):
    pass


connection, cursor = connect_postgresql_local_server()

stock_data_directory = Path("data")
for ticker in tickers:
    create_stock_data_table(
        connection, cursor, ticker
    )  # Create blank table if table for stock does not exist
    collect_stock_data = CollectDailyData(ticker)
    stock_history = collect_stock_data.get_ticker_history()
transform_stock_data(cursor, ticker)
