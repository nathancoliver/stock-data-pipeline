"""Main script to run the stock data pipeline.
Download stock data from Yahoo Finance, transform data in SQL, and load data into AWS.
"""

import os
import time
import datetime
from pathlib import Path
from typing import List
from stock_data_pipeline.load_yfinance_data import CollectDailyData
from shutil import rmtree
import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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
    "date": sqlalchemy.DATE,
    "open": sqlalchemy.types.Numeric(10, 2),
    "high": sqlalchemy.types.Numeric(10, 2),
    "low": sqlalchemy.types.Numeric(10, 2),
    "close": sqlalchemy.types.Numeric(10, 2),
    "volume": sqlalchemy.types.BigInteger,
}
SECTOR_WEIGHTS_DTYPES = {
    "symbol": sqlalchemy.VARCHAR(6),
    "index_weight": sqlalchemy.types.Numeric(10, 4),
}
stock_weight_directory = Path("stock_weights")


def create_directory(directory_path):
    if directory_path.exists():
        rmtree(directory_path)
    directory_path.mkdir(exist_ok=True)


def get_list_of_tickers_from_txt(file_path: Path) -> List[str]:
    tickers = []
    with open(file_path, "r", encoding="utf-8") as file:
        for ticker in file:
            tickers.append(ticker.rstrip("\n").lower())
    return tickers


def set_download_directory(download_directory: str):

    options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": download_directory}
    options.add_experimental_option("prefs", prefs)
    return options


def download_file_from_website(url: str, options, xpath: str, file_path: Path):

    service = Service(executable_path="chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)
    wait = WebDriverWait(driver, 10)  # Wait up to 10 seconds.
    csv_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    )  # Wait until button is clickable.
    ActionChains(driver).move_to_element(csv_button).click().perform()  # Click button.
    while not file_path.exists():  # Wait until file is downloaded.
        time.sleep(0.1)
    driver.quit()  # Quit driver.


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


create_directory(stock_weight_directory)
config_directory = Path("config")
sectors_file_name = "spdr_sectors.txt"
sectors_file_path = Path(config_directory, sectors_file_name)
sectors = get_list_of_tickers_from_txt(sectors_file_path)  # Get list of sectors

download_directory = f"{os.getcwd()}\\{stock_weight_directory}"
options = set_download_directory(download_directory)

for sector in sectors:
    xpath = "//h2[contains(text(), 'Holdings')]/ancestor::div[2]//button[contains(text(), 'CSV File')]"
    url = f"https://www.sectorspdrs.com/mainfund/{sector}"
    sector_weights_file_path = Path(
        stock_weight_directory, f"index-holdings-{sector}.csv"
    )
    download_file_from_website(url, options, xpath, sector_weights_file_path)


connection, cursor = (
    connect_postgresql_local_server()
)  # Get postgreSQL connection and cursor.
engine = create_engine(SQLALCHEMY_CONNECTION_STRING)  # Get SQLAlchemy engine.

tickers: set[str] = set()
for sector in sectors:
    sector_weights_file_path = Path(
        stock_weight_directory, f"index-holdings-{sector}.csv"
    )
    df_weights = pd.read_csv(sector_weights_file_path, header=1)[
        ["Symbol", "Index Weight"]
    ]
    df_weights.columns = [
        column.lower().replace(" ", "_") for column in df_weights.columns
    ]
    df_weights["symbol"] = df_weights["symbol"].str.lower()
    df_weights["index_weight"] = (
        df_weights["index_weight"].str.rstrip("%").astype(float) / 100
    )
    tickers_sector = set(df_weights["symbol"])
    tickers = tickers | tickers_sector
    df_weights.index = df_weights["symbol"]
    df_weights = df_weights.drop(labels="symbol", axis=1)
    df_weights.to_sql(
        f"{sector}_weights",
        con=engine,
        if_exists="replace",
        index=True,
        index_label="symbol",
        dtype=SECTOR_WEIGHTS_DTYPES,
    )

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
