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
stocks = []
config_directory = Path("config")
stocks_file_path = Path(config_directory, "stocks.txt")
with open(stocks_file_path, "r", encoding="utf-8") as file:
    for stock in file:
        stocks.append(stock.rstrip("\n"))

def connect_postgresql_local_server():
    connection = psycopg2.connect(**DB_PARAMS)
    return connection, connection.cursor()

stock_data_directory = Path("data")
for stock in stocks:
    collect_stock_data = CollectDailyData(stock, stock_data_directory)
    stock_history = collect_stock_data.get_ticker_history()


print()
