"""Main script to run the stock data pipeline.
Download stock data from Yahoo Finance, transform data in SQL, and load data into AWS.
"""

import os
import time
import datetime
from enum import Enum
from pathlib import Path
from typing import List, Dict
from stock_data_pipeline.load_yfinance_data import CollectDailyData
from shutil import rmtree
import psycopg2  # type: ignore
import pandas as pd  # type: ignore
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

database_parameters: Dict[str, str] = {
    "host": HOST,
    "port": PORT,
    "dbname": DATABASE,
    "user": USER,
    "password": PASSWORD,
}
engine_parameters = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
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
    "weight": sqlalchemy.types.Numeric(10, 4),
    "shares_held": sqlalchemy.types.BigInteger,
}
stock_weight_directory = Path("stock_weights")
config_directory = "config"
sectors_file_name = "spdr_sectors.txt"
sectors_file_path = Path(config_directory, sectors_file_name)


def make_ticker_sql_compatible(name: str) -> str:
    return name.replace(".", "_").lower()


def make_ticker_yfinance_compatible(name: str) -> str:
    return name.replace(".", "-")


def convert_shares_outstanding(shares_outstanding: str) -> int:
    magnitude = shares_outstanding.rstrip(" ")[-1].upper()
    value = float(shares_outstanding.rstrip(magnitude).strip(" "))
    if magnitude == "M":
        return int(value * 10**6)
    elif magnitude == "B":
        return int(value * 10**9)
    else:
        raise NameError(
            f"magnitude {magnitude} from shares_outstanding is not compatible with func convert_shares_outstanding. Consider editing func."
        )


class SQLOperation(Enum):
    EXECUTE = "execute"
    COMMIT = "commit"


class PostgreSQLConnection:

    def __init__(self, database_parameters: Dict[str, str], engine_parameters: str):
        self.connection: psycopg2.extensions.connection = psycopg2.connect(
            **database_parameters
        )
        self.cursor: psycopg2.extensions.cursor = self.connection.cursor()
        self.engine = create_engine(engine_parameters)

    def execute_query(self, query, operation: SQLOperation, values=None):
        """Execute postgreSQL query."""

        if values:
            self.cursor.execute(query, values)  # Use values to parameterize the query
        else:
            self.cursor.execute(query)

        if operation == SQLOperation.COMMIT:
            self.connection.commit()
        elif operation == SQLOperation.EXECUTE:
            return self.cursor
        else:
            raise NameError(f"operation {SQLOperation} is not a valid input.")

    def set_primary_key(self, table_name: str, column: str) -> None:
        query = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column})"
        self.execute_query(query, SQLOperation.COMMIT)


class ChromeDriver:

    def __init__(self, download_file_directory: str | Path):
        self.download_file_directory_str = download_file_directory
        self.download_file_directory_path = Path(download_file_directory)
        self.download_file_directory_absolute_path = (
            f"{os.getcwd()}\\{download_file_directory}"
        )

        # Update ChromeDriver preferences to download files to self.download_file_directory
        options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": self.download_file_directory_absolute_path
        }
        options.add_experimental_option("prefs", prefs)

        service = Service(executable_path="chromedriver.exe")
        self.driver = webdriver.Chrome(service=service, options=options)

        self.wait = WebDriverWait(self.driver, 10)

    def create_directory(self):
        if self.download_file_directory_path.exists():
            rmtree(self.download_file_directory_path)
        self.download_file_directory_path.mkdir(exist_ok=True)

    def load_url(self, url: str):
        self.driver.get(url)
        time.sleep(2)

    def press_button(self, xpath):
        button = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )  # Wait until button is clickable.
        ActionChains(self.driver).move_to_element(
            button
        ).click().perform()  # Click button.

    def quit_driver(self):
        self.driver.quit()


class Ticker:

    def __init__(self, ticker: str, postgresql_connection: PostgreSQLConnection):
        self.ticker = make_ticker_sql_compatible(ticker)
        self.yfinance_ticker = make_ticker_yfinance_compatible(ticker)
        self.table_name = f"{self.ticker}_stock_history"
        self.postgresql_connection = postgresql_connection
        self.stock_history = pd.DataFrame()

        query = f"CREATE TABLE IF NOT EXISTS {self.table_name} (date DATE PRIMARY KEY,open NUMERIC(10, 2),high NUMERIC(10, 2),low NUMERIC(10, 2),close NUMERIC(10, 2),volume BIGINT)"
        self.postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)

    def get_latest_date(self) -> datetime.date | None:
        """Get most recent date from stock history. If no stock history, return None."""

        query = f"SELECT MAX(DATE) FROM {self.table_name}"
        cursor = self.postgresql_connection.execute_query(
            query, operation=SQLOperation.EXECUTE
        )
        return cursor.fetchone()[0]


class Tickers:

    def __init__(self):
        self.tickers: Dict[str, Ticker] = {}

    def add_ticker(self, ticker_str: str, ticker_object):
        if ticker_str not in self.tickers:
            self.tickers.update({ticker_str: ticker_object})


class Sector:

    download_file_directory_path: Path

    def __init__(
        self,
        sector: str,
        chrome_driver: ChromeDriver,
        postgresql_connection: PostgreSQLConnection,
    ):
        self.download_file_directory_path = chrome_driver.download_file_directory_path
        self.sector = make_ticker_sql_compatible(sector)
        self.postgresql_connection = postgresql_connection
        self.shares_outstanding: None | int = None
        self.index_holdings_file_path: Path = Path(
            self.download_file_directory_path, f"index-holdings-{self.sector}.csv"
        )
        self.portfolio_holdings_file_path: Path = Path(
            self.download_file_directory_path, f"portfolio-holdings-{self.sector}.csv"
        )
        self.tickers: List[Ticker] = []
        self.shares_outstanding: None | int = None
        self.shares_outstanding_xpath = (
            "//dt[text()='Shares Outstanding']/following-sibling::dd"
        )
        self.index_csv_xpath = "(//span[contains(text(), 'Download a Spreadsheet')]/following-sibling::button[contains(text(), 'CSV File')])[1]"
        self.portfolio_tab_xpath = "//a[contains(text(), 'Portfolio Holdings')]"
        self.portfolio_csv_xpath = "(//span[contains(text(), 'Download a Spreadsheet')]/following-sibling::button[contains(text(), 'CSV File')])[2]"

    def add_ticker(self, ticker_object: Ticker):
        if ticker_str not in self.tickers:
            self.tickers.append(ticker_object)

    def create_sector_history_table(self, number_of_days):

        first_ticker_table_name = self.tickers[0].table_name
        table_name_query = (
            f"CREATE TABLE IF NOT EXISTS {self.sector_sector_history_table_name} as"
        )
        select_query = f" SELECT {first_ticker_table_name}.date as date"
        column_query = (
            f", {first_ticker_table_name}.close as {first_ticker_table_name}_close"
        )
        from_query = f" FROM {first_ticker_table_name}"
        join_query = ""
        where_query = f" ORDER BY {first_ticker_table_name}.date ASC"
        limit_query = f" LIMIT {number_of_days}"
        for ticker in self.tickers[1:]:
            ticker_table_name = ticker.table_name
            column_query += f", {ticker_table_name}.close as {ticker_table_name}_close"
            join_query += f" JOIN {ticker_table_name} ON {first_ticker_table_name}.date = {ticker_table_name}.date"
        query = (
            table_name_query
            + select_query
            + column_query
            + from_query
            + join_query
            + where_query
            + limit_query
        )
        self.postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)


class Sectors(Sector):

    def __init__(
        self,
        file_path: Path,
        chrome_driver: ChromeDriver,
        postgresql_connection: PostgreSQLConnection,
    ):
        self.sectors: List[Sector] = []
        self.shares_outstanding: Dict[str, List[str | int]] = {
            "sector": [],
            "shares_outstanding": [],
        }

        with open(file_path, "r", encoding="utf-8") as file:
            for sector_ticker in file:
                self.sectors.append(
                    Sector(
                        sector_ticker.rstrip("\n"),
                        chrome_driver=chrome_driver,
                        postgresql_connection=postgresql_connection,
                    )
                )

    def append_shares_outstanding_dict(self, sector: Sector, shares_outstanding: int):
        self.shares_outstanding["sector"].append(sector.sector_symbol)
        self.shares_outstanding["shares_outstanding"].append(shares_outstanding)

    def create_shares_outstanding_table(self):
        # TODO: initilalize sql table (create table if exists ...)
        # TODO: create a row of shares outstanding and add row to existing sql tables
        end_date = datetime.datetime.now()
        date_range = [
            (end_date - datetime.timedelta(days=day)).strftime("%Y-%m-%d")
            for day in range(365)
        ]
        temporary_dict = {"date": date_range}
        number_of_dates = len(date_range)
        shares_outstanding_dtypes = {
            "date": sqlalchemy.DATE,
        }
        for sector in self.sectors:
            temporary_dict.update(
                {sector.sector_symbol: [sector.shares_outstanding] * number_of_dates}
            )
            shares_outstanding_dtypes.update(
                {
                    sector.sector_symbol: sqlalchemy.types.BigInteger,
                }
            )
        pd.DataFrame(temporary_dict).set_index("date").to_sql(
            "sector_shares_outstanding",
            con=postgresql_connection.engine,
            if_exists="replace",  # TODO: eventually this will need to be replaced with
            index=True,
            index_label="date",
            dtype=shares_outstanding_dtypes,
        )


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


chrome_driver = ChromeDriver(stock_weight_directory)
chrome_driver.create_directory()

postgresql_connection = PostgreSQLConnection(database_parameters, engine_parameters)

sectors = Sectors(
    sectors_file_path,
    chrome_driver=chrome_driver,
    postgresql_connection=postgresql_connection,
)
tickers = Tickers()

for sector in sectors.sectors:

    chrome_driver.load_url(sector.url)
    shares_outstanding = convert_shares_outstanding(
        chrome_driver.driver.find_element(
            By.XPATH, sector.shares_outstanding_xpath
        ).text
    )
    sectors.append_shares_outstanding_dict(sector, shares_outstanding)
    sector.shares_outstanding = shares_outstanding
    chrome_driver.press_button(sector.index_csv_xpath)
    while (
        not sector.index_holdings_file_path.exists()
    ):  # Wait until file is downloaded.
        time.sleep(0.1)
    chrome_driver.press_button(sector.portfolio_tab_xpath)
    chrome_driver.press_button(sector.portfolio_csv_xpath)
    while (
        not sector.portfolio_holdings_file_path.exists()
    ):  # Wait until file is downloaded.
        time.sleep(0.1)

chrome_driver.quit_driver()  # Quit driver.

for sector in sectors.sectors:

    df_sector_shares = pd.read_csv(sector.portfolio_holdings_file_path, header=1)[
        ["Symbol", "Weight", "Shares Held"]
    ]
    df_sector_shares.columns = [
        column.lower().replace(" ", "_") for column in df_sector_shares.columns
    ]
    df_sector_shares = df_sector_shares[df_sector_shares["symbol"].notna()]
    df_sector_shares = df_sector_shares[~df_sector_shares["symbol"].str.contains("25")]
    df_sector_shares["symbol"] = df_sector_shares["symbol"].str.lower()
    df_sector_shares["weight"] = (
        df_sector_shares["weight"].str.rstrip("%").astype(float) / 100
    )
    df_sector_shares["shares_held"] = (
        df_sector_shares["shares_held"].str.replace(",", "").astype(int)
    )
    tickers_in_sector = set(df_sector_shares["symbol"])
    for ticker_str in tickers_in_sector:
        ticker_object = Ticker(ticker_str, postgresql_connection)
        sector.add_ticker(ticker_object)
        tickers.add_ticker(ticker_str, ticker_object)
    df_sector_shares.index = df_sector_shares["symbol"]
    df_sector_shares = df_sector_shares.drop(labels="symbol", axis=1)
    df_sector_shares.to_sql(
        make_ticker_sql_compatible(sector.sector_shares_table_name),
        con=postgresql_connection.engine,
        if_exists="replace",
        index=True,
        index_label="symbol",
        dtype=SECTOR_WEIGHTS_DTYPES,
    )
    postgresql_connection.set_primary_key(
        sector.sector_shares_table_name, column="symbol"
    )

sectors.create_shares_outstanding_table()

# Create or append stock history table for each ticker.
for ticker in tickers.tickers.values():

    latest_date = ticker.get_latest_date()  # Get latest date of stock history table.
    collect_stock_data = CollectDailyData(
        ticker.yfinance_ticker, latest_date=latest_date
    )  # initialize CollectDailyData class.
    ticker.stock_history = (
        collect_stock_data.get_ticker_history()
    )  # retrieve stock history pd.DataFrame.
    if ticker.stock_history is not None:
        ticker.stock_history.columns = [
            column.lower() for column in ticker.stock_history.columns
        ]  # Set column names to all lower-case letters.
        ticker.stock_history.index.name = (
            ticker.stock_history.index.name.lower()
        )  # Set date index to lower-case letters.
        stock_history = check_table_append_compatibility(
            latest_date, ticker.stock_history
        )  # Filter stock history to ensure no overlapping dates in postgreSQL table.
        # Skip add_data if stock history table is empty.
        if not stock_history.empty:
            stock_history.to_sql(
                ticker.table_name,
                con=ticker.postgresql_connection.engine,
                if_exists="append",
                index=True,
                index_label="date",
                dtype=STOCK_HISTORY_DTYPES,
            )  # Append data to stock history table.

for sector in sectors.sectors:
    sector.create_sector_history_table(30)
