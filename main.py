"""Main script to run the stock data pipeline.
Download stock data from Yahoo Finance, transform data in SQL, and load data into AWS.
"""

import time
from pathlib import Path
from typing import Dict
import sqlalchemy


from selenium.webdriver.common.by import By


from stock_data_pipeline.chrome_driver import ChromeDriver
from stock_data_pipeline.functions import (
    check_table_append_compatibility,
    get_sql_table_latest_date,
    get_todays_date,
    get_market_day,
    initialize_table,
    make_ticker_sql_compatible,
    set_table_primary_key,
)
from stock_data_pipeline.postgresql_connection import PostgreSQLConnection
from stock_data_pipeline.sectors import Sectors
from stock_data_pipeline.ticker import Ticker
from stock_data_pipeline.tickers import Tickers
from stock_data_pipeline.load_yfinance_data import CollectDailyData


HOST = "localhost"
PORT = 5432
DATABASE = "stock_history"
USER = "postgres"
PASSWORD = "l"


database_parameters: Dict[str, str | int] = {
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


chrome_driver = ChromeDriver(stock_weight_directory)
chrome_driver.create_directory()

postgresql_connection = PostgreSQLConnection(database_parameters, engine_parameters)

sectors = Sectors(
    sectors_file_path,
    chrome_driver=chrome_driver,
    postgresql_connection=postgresql_connection,
)
tickers = Tickers()

market_day = get_market_day(get_todays_date())

if market_day:
    for sector in sectors.sectors:

        chrome_driver.load_url(sector.url)
        shares_outstanding = sectors.convert_shares_outstanding(
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
        df_sector_shares = sector.create_sector_shares_dataframe(todays_date)
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
            sector_weights_dtypes.update(
                {ticker_symbol: sqlalchemy.types.BigInteger}
            )  # TODO: Move this to Sector class, specifically init function and add_ticker func.

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
