"""Main script to run the stock data pipeline.
Download stock data from Yahoo Finance, transform data in SQL, and upload data to AWS.
"""

import os
from pathlib import Path
import time
from typing import Dict
import sqlalchemy


from selenium.webdriver.common.by import By

from stock_data_pipeline import (
    ChromeDriver,
    PostgreSQLConnection,
    S3Connection,
    Sectors,
    Ticker,
    Tickers,
    CollectDailyData,
    check_table_append_compatibility,
    get_environment_variable,
    get_market_day,
    get_s3_table_latest_date,
    get_todays_date,
    initialize_table,
    make_ticker_sql_compatible,
    set_table_primary_key,
)

AWS_ACCESS_KEY = get_environment_variable("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = get_environment_variable("AWS_SECRET_ACCESS_KEY")
AWS_USERNAME = get_environment_variable("AWS_USERNAME")
STOCK_DATA_PIPELINE_BUCKET_NAME = get_environment_variable(
    "STOCK_DATA_PIPELINE_BUCKET_NAME"
)
STOCK_DATA_PIPELINE_BUCKET_REGION_NAME = get_environment_variable(
    "STOCK_DATA_PIPELINE_BUCKET_REGION_NAME"
)

POSTGRESQL_HOST = get_environment_variable(
    "POSTGRESQL_HOST", alternative_name="localhost"
)
POSTGRESQL_PORT = get_environment_variable("POSTGRESQL_PORT", alternative_name="5432")
POSTGRESQL_STOCK_DATA_PIPELINE_DATABASE = get_environment_variable(
    "POSTGRESQL_STOCK_DATA_PIPELINE_DATABASE", alternative_name="mydatabase"
)
POSTGRESQL_USER = get_environment_variable(
    "POSTGRESQL_USER", alternative_name="postgres"
)
POSTGRESQL_PASSWORD = get_environment_variable(
    "POSTGRESQL_PASSWORD", alternative_name="postgres"
)


database_parameters: Dict[str, str | int] = {
    "host": POSTGRESQL_HOST,
    "port": POSTGRESQL_PORT,
    "dbname": POSTGRESQL_STOCK_DATA_PIPELINE_DATABASE,
    "user": POSTGRESQL_USER,
    "password": POSTGRESQL_PASSWORD,
}
engine_parameters = f"postgresql+psycopg2://{POSTGRESQL_USER}:{POSTGRESQL_PASSWORD}@{POSTGRESQL_HOST}:{POSTGRESQL_PORT}/{POSTGRESQL_STOCK_DATA_PIPELINE_DATABASE}"
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
s3_connection = S3Connection(
    stock_weight_directory=stock_weight_directory,
    AWS_ACCESS_KEY=AWS_ACCESS_KEY,
    AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY,
    STOCK_DATA_PIPELINE_BUCKET_NAME=STOCK_DATA_PIPELINE_BUCKET_NAME,
    STOCK_DATA_PIPELINE_BUCKET_REGION_NAME=STOCK_DATA_PIPELINE_BUCKET_REGION_NAME,
    AWS_USERNAME=AWS_USERNAME,
)

sectors = Sectors(
    sectors_file_path,
    chrome_driver=chrome_driver,
    postgresql_connection=postgresql_connection,
    s3_connection=s3_connection,
)
tickers = Tickers()

market_day = get_market_day(get_todays_date())

if market_day:
    for sector in sectors.sectors:
        print(f"Start scraping {sector.sector_symbol} sector info.")
        chrome_driver.load_url(sector.url)
        time.sleep(5)
        shares_outstanding = sectors.convert_shares_outstanding(
            chrome_driver.driver.find_element(
                By.XPATH, sector.shares_outstanding_xpath
            ).text
        )
        sectors.append_shares_outstanding_dict(sector, shares_outstanding)
        sector.shares_outstanding = shares_outstanding
        chrome_driver.scroll_window(700)
        time.sleep(5)
        chrome_driver.press_button(
            cell_type=By.XPATH, path=sector.portfolio_tab_path, element_index=1
        )
        time.sleep(5)
        chrome_driver.press_button(
            cell_type=By.XPATH, path=sector.portfolio_csv_path, element_index=2
        )
        while (
            not sector.portfolio_holdings_file_path.exists()
        ):  # Wait until file is downloaded.
            time.sleep(0.1)
        print(f"End scraping {sector.sector_symbol} sector info.")

    chrome_driver.quit_driver()
    print("Quit driver.")

    for sector in sectors.sectors:
        initialize_table(
            sector.sector_shares_table_name,
            sector.sector_shares_data_types,
            postgresql_connection=sector.postgresql_connection,
        )
        todays_date = get_todays_date()
        df_sector_shares = sector.create_sector_shares_dataframe(todays_date)
        latest_date = get_sql_table_latest_date(  # TODO: Need to get dataframe from S3 bucket. If no database, return None.
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

        if latest_date is None:
            set_table_primary_key(
                sector.sector_shares_table_name, "date", postgresql_connection
            )
        elif (
            todays_date > latest_date
        ):  # TODO: If date is None, error. Need to fix, probably with If latest_date is None, elif ...
            df_sector_shares.to_sql(
                make_ticker_sql_compatible(sector.sector_shares_table_name),
                con=postgresql_connection.engine,
                if_exists="append",
                index=True,
                index_label="date",
                dtype=sector_weights_dtypes,
            )

    sectors.create_shares_outstanding_table()

    # Create or append stock history table for each ticker.
    for ticker in tickers.tickers.values():
        print(f"Start retrieve {ticker.ticker_symbol} stock history.")
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
