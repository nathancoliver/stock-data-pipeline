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

SECTOR_SHARES_OUTSTANDING = "sector_shares_outstanding"

database_parameters: Dict[str, str] = {
    "host": HOST,
    "port": PORT,
    "dbname": DATABASE,
    "user": USER,
    "password": PASSWORD,
}
engine_parameters = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
stock_history_dtypes = {
    "date": sqlalchemy.DATE,
    "open": sqlalchemy.types.Numeric(10, 2),
    "high": sqlalchemy.types.Numeric(10, 2),
    "low": sqlalchemy.types.Numeric(10, 2),
    "close": sqlalchemy.types.Numeric(10, 2),
    "volume": sqlalchemy.types.BigInteger,
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


def get_todays_date():
    # TODO: below is a temporary solution, will need to be adjusted depending on what time the CI runs
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    weekday = today.weekday()
    if weekday >= 5:  # if Saturday to Sunday
        weekday_adjustment = weekday - 4
    elif weekday == 0:  # if Monday
        weekday_adjustment = 3
    else:  # if Tuesday to Friday
        weekday_adjustment = 1
    return today - datetime.timedelta(days=weekday_adjustment)


def get_sql_table_latest_date(
    table_name: str, engine: sqlalchemy.engine.Engine
) -> datetime.datetime | None:

    try:
        df_shares_outstanding_shares_exists = pd.read_sql(table_name, engine)
        latest_date = (
            df_shares_outstanding_shares_exists.set_index("date")
            .sort_index(ascending=False)
            .index[0]
        ).to_pydatetime()  # TODO: figure out how to type hint this date
        return latest_date
    except:
        return None


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


def set_table_primary_key(
    table_name: str, primary_key: str, postgresql_connection: PostgreSQLConnection
) -> None:
    postgresql_connection.set_primary_key(table_name, column=primary_key)


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


class Tickers:

    def __init__(self):
        self.tickers: Dict[str, Ticker] = {}

    def add_ticker(self, ticker_symbol: str, ticker_object):
        if ticker_symbol not in self.tickers:
            self.tickers.update({ticker_symbol: ticker_object})


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

    def add_ticker(self, ticker_object: Ticker):
        if ticker_object.ticker_symbol not in self.tickers:
            self.tickers.append(ticker_object)

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


class Sectors:

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
        initialize_table(
            SECTOR_SHARES_OUTSTANDING,
            self.sector_shares_outstanding_dtypes,
            postgresql_connection=self.postgresql_connection,
        )
        # TODO: initilalize sql table (create table if exists ...)
        # TODO: create a row of shares outstanding and add row to existing sql tables
        todays_date = get_todays_date()
        latest_date = get_sql_table_latest_date(
            SECTOR_SHARES_OUTSTANDING, postgresql_connection.engine
        )
        shares_outstanding = {"date": [todays_date]}
        shares_outstanding_dtypes = {
            "date": sqlalchemy.DATE,
        }
        for sector in self.sectors:
            shares_outstanding.update(
                {sector.sector_symbol: [sector.shares_outstanding]}
            )
            shares_outstanding_dtypes.update(
                {
                    sector.sector_symbol: sqlalchemy.types.BigInteger,
                }
            )
        if todays_date > latest_date:
            pd.DataFrame(shares_outstanding).set_index("date").to_sql(
                SECTOR_SHARES_OUTSTANDING,
                con=postgresql_connection.engine,
                if_exists="append",  # TODO: eventually this will need to be replaced with append
                index=True,
                index_label="date",
                dtype=shares_outstanding_dtypes,
            )
        if latest_date is None:
            set_table_primary_key(
                SECTOR_SHARES_OUTSTANDING, "date", self.postgresql_connection
            )


def initialize_table(
    table_name: str,
    data_types: Dict[str, DataTypes],
    postgresql_connection: PostgreSQLConnection,
) -> None:
    dtypes_string = convert_sql_data_type_into_string(data_types)
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({dtypes_string})"
    postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)


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


def create_sector_shares_dataframe(
    sector: Sector, todays_date: datetime.datetime
) -> pd.DataFrame:

    df_sector_shares = pd.read_csv(sector.portfolio_holdings_file_path, header=1)[
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

chrome_driver.quit_driver()

for sector in sectors.sectors:
    initialize_table(
        sector.sector_shares_table_name,
        sector.sector_shares_data_types,
        postgresql_connection=sector.postgresql_connection,
    )
    todays_date = get_todays_date()
    df_sector_shares = create_sector_shares_dataframe(sector, todays_date)
    latest_date = get_sql_table_latest_date(
        sector.sector_shares_table_name, postgresql_connection.engine
    )

    tickers_in_sector = set(df_sector_shares.columns)
    sector_weights_dtypes = {"date": sqlalchemy.Date}
    for (
        ticker_symbol
    ) in (
        tickers_in_sector
    ):  # TODO: check if for loop can be skipped in certain situations (e.g. latest_date is None)
        ticker_object = Ticker(ticker_symbol, postgresql_connection)
        sector.add_ticker(ticker_object)
        tickers.add_ticker(ticker_symbol, ticker_object)
    if todays_date > latest_date:  # TODO: fix date comparison
        df_sector_shares.to_sql(
            make_ticker_sql_compatible(sector.sector_shares_table_name),
            con=postgresql_connection.engine,
            if_exists="append",
            index=True,
            index_label="date",
            dtype=sector_weights_dtypes,
        )
    if latest_date is None:
        set_table_primary_key(
            sector.sector_shares_table_name, "date", postgresql_connection
        )

sectors.create_shares_outstanding_table()

# Create or append stock history table for each ticker.
for ticker in tickers.tickers.values():

    latest_date = (
        ticker.get_stock_history_latest_date()
    )  # Get latest date of stock history table.
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
                dtype=stock_history_dtypes,
            )  # Append data to stock history table.

for sector in sectors.sectors:
    sector.create_sector_history_table()
