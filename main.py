"""Main script to run the stock data pipeline.
Download stock data from Yahoo Finance, transform data in SQL, and upload data to AWS.
"""

import os
from pathlib import Path
import time
from typing import Dict
import requests
import sqlalchemy

from stock_data_pipeline import (
    CollectDailyData,
    DataTypes,
    PostgreSQLConnection,
    S3Connection,
    Sectors,
    SQLOperation,
    TickerColumnType,
    Ticker,
    Tickers,
    STOCK_WEIGHT_DIRECTORY,
    check_table_append_compatibility,
    create_directory,
    get_environment_variable,
    get_market_day,
    get_s3_table,
    get_todays_date,
    initialize_table,
    make_ticker_sql_compatible,
)

AWS_ACCESS_KEY = get_environment_variable("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = get_environment_variable("AWS_SECRET_ACCESS_KEY")
AWS_USERNAME = get_environment_variable("AWS_USERNAME")
STOCK_DATA_PIPELINE_BUCKET_NAME = get_environment_variable("STOCK_DATA_PIPELINE_BUCKET_NAME")
STOCK_DATA_PIPELINE_BUCKET_REGION_NAME = get_environment_variable("STOCK_DATA_PIPELINE_BUCKET_REGION_NAME")

POSTGRESQL_HOST = get_environment_variable("POSTGRESQL_HOST", alternative_name="localhost")
POSTGRESQL_PORT = get_environment_variable("POSTGRESQL_PORT", alternative_name="5432")
POSTGRESQL_DB = get_environment_variable("POSTGRESQL_DB")
POSTGRESQL_USER = get_environment_variable("POSTGRESQL_USER", alternative_name="postgres")
POSTGRESQL_PASSWORD = get_environment_variable("POSTGRESQL_PASSWORD")


database_parameters: Dict[str, str | int] = {
    "host": POSTGRESQL_HOST,
    "port": POSTGRESQL_PORT,
    "dbname": POSTGRESQL_DB,
    "user": POSTGRESQL_USER,
    "password": POSTGRESQL_PASSWORD,
}
engine_parameters = f"postgresql+psycopg2://{POSTGRESQL_USER}:{POSTGRESQL_PASSWORD}@{POSTGRESQL_HOST}:{POSTGRESQL_PORT}/{POSTGRESQL_DB}"
stock_history_dtypes = {
    "date": sqlalchemy.DATE,
    "open": sqlalchemy.types.Numeric(10, 2),
    "high": sqlalchemy.types.Numeric(10, 2),
    "low": sqlalchemy.types.Numeric(10, 2),
    "close": sqlalchemy.types.Numeric(10, 2),
    "volume": sqlalchemy.types.BigInteger,
}

DATA_DIRECTORY = Path("data")
config_directory = "config"
sectors_file_name = "spdr_sectors.txt"
sectors_file_path = Path(config_directory, sectors_file_name)

create_directory(DATA_DIRECTORY)
create_directory(STOCK_WEIGHT_DIRECTORY)

postgresql_connection = PostgreSQLConnection(database_parameters, engine_parameters)
s3_connection = S3Connection(
    stock_weight_directory=STOCK_WEIGHT_DIRECTORY,
    data_directory=DATA_DIRECTORY,
    AWS_ACCESS_KEY=AWS_ACCESS_KEY,
    AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY,
    STOCK_DATA_PIPELINE_BUCKET_NAME=STOCK_DATA_PIPELINE_BUCKET_NAME,
    STOCK_DATA_PIPELINE_BUCKET_REGION_NAME=STOCK_DATA_PIPELINE_BUCKET_REGION_NAME,
    AWS_USERNAME=AWS_USERNAME,
)

sectors = Sectors(
    sectors_file_path,
    postgresql_connection=postgresql_connection,
    s3_connection=s3_connection,
)
tickers = Tickers()

# TODO: need to update function that retrieves todays date
todays_date = get_todays_date()
market_day = get_market_day(todays_date)

print(f"todays adjusted date {todays_date}")

if market_day:
    for sector in sectors.sectors:
        print(f"Start scraping {sector.sector_symbol} sector info.")

        response = requests.get(sector.url_shares_outstanding)
        shares_outstanding_text = sector.parse_shares_outstanding(response.text)
        time.sleep(2)
        shares_outstanding = sectors.convert_shares_outstanding(shares_outstanding_text)
        sectors.append_shares_outstanding_dict(sector, shares_outstanding)
        sector.shares_outstanding = shares_outstanding
        response = requests.get(sector.url_xlsx)
        with open(
            f"{STOCK_WEIGHT_DIRECTORY}/holdings-daily-us-en-{sector.sector_symbol}.xlsx",
            "wb",
        ) as f:
            f.write(response.content)
        print(f"End scraping {sector.sector_symbol} sector info.")

    print("Quit driver.")
    for sector in sectors.sectors:
        postgresql_connection.execute_query(
            f"DROP TABLE IF EXISTS {sector.sector_shares_table_name}",
            operation=SQLOperation.COMMIT,
        )
    postgresql_connection.execute_query(
        f"DROP TABLE IF EXISTS sector_shares_outstanding",
        operation=SQLOperation.COMMIT,
    )
    for sector in sectors.sectors:
        if sector.sector_symbol == "xlc":
            print()
        sector.sector_shares_df = get_s3_table(
            sector.s3_connection,
            s3_file_name=sector.sector_shares_s3_file_name,
            download_file_path=sector.sector_shares_download_file_path,
        )  # Download S3 table and create Pandas table TODO: Need to return None if CSV table in S3 does not exist.
        sector.sector_shares_df.drop(columns=[column for column in sector.sector_shares_df if "_shares_shares" in column], inplace=True)
        original_tickers = [column.replace("_shares", "", count=-1) for column in sector.sector_shares_df.columns]

        latest_sector_shares = sector.create_sector_shares_dataframe(todays_date)
        latest_sector_shares.columns = [f"{column}_shares" for column in latest_sector_shares]
        latest_tickers = [column.replace("_shares", "", count=-1) for column in latest_sector_shares.columns]
        sector.old_tickers = [column.replace("_shares", "", count=-1) for column in sector.sector_shares_df.columns if column not in latest_sector_shares.columns]
        if sector.old_tickers:
            sector.sector_shares_df.drop(labels=[f"{ticker}_shares" for ticker in sector.old_tickers], axis=1, inplace=True)

        sector_weights_dtypes = {"date": sqlalchemy.types.Date}
        sector_weights_dtypes_strings = {
            "date": DataTypes.DATE,
        }
        sector.get_new_tickers(original_tickers=original_tickers, latest_tickers=latest_tickers)
        tickers_in_sector = [ticker_shares.replace("_shares", "", count=-1) for ticker_shares in set(latest_sector_shares.columns)]
        tickers_in_sector.extend(sector.new_tickers)
        for ticker_symbol in set(tickers_in_sector):
            ticker_object = Ticker(ticker_symbol, postgresql_connection)
            sector.add_ticker(ticker_object)
            tickers.add_ticker(ticker_symbol, ticker_object)
            sector_weights_dtypes.update({ticker_object.shares_column_name: sqlalchemy.types.BigInteger})  # TODO: Move this to Sector class, specifically init function and add_ticker func.
            sector_weights_dtypes_strings.update(
                {
                    ticker_object.shares_column_name: DataTypes.BIGINT,
                }
            )
        sector_weights_dtypes.update({"date": sqlalchemy.types.Date})
        sector_weights_dtypes_strings.update(
            {
                "date": DataTypes.DATE,
            }
        )
        initialize_table(  # Create SQL table. This does not append latest sector shares data, only creates SQL table.
            table_name=sector.sector_shares_table_name,
            data_types=sector_weights_dtypes,
            data_types_strings=sector_weights_dtypes_strings,
            postgresql_connection=postgresql_connection,
            data_frame=sector.sector_shares_df,
        )

        latest_date = sector.get_s3_table_latest_date()
        print(
            f"sector: {sector.sector_symbol}",
            f"today's date: {todays_date}",
            f"latest_date: {latest_date}",
            sep="\n",
        )
        if todays_date > latest_date:  # TODO: If date is None, error. Need to fix, probably with If latest_date is None, elif ...
            # sector.new_tickers = [column for column in latest_sector_shares.columns if column not in sector.sector_shares_df.columns]
            sector.add_missing_columns(
                column_type=TickerColumnType.SHARES,
                sql_table_name=sector.sector_shares_table_name,
                data_type_string=DataTypes.BIGINT,
                postgresql_connection=postgresql_connection,
            )
            latest_sector_shares.to_sql(
                make_ticker_sql_compatible(sector.sector_shares_table_name),
                con=postgresql_connection.engine,
                if_exists="append",  # TODO: need to figure out why append does not currently work. Should be able to take one row sector shares df and append to SQL table.
                index=True,
                index_label="date",
                dtype=sector_weights_dtypes,
            )
            sector.s3_connection.upload_sql_table(
                sector.sector_shares_table_name,
                sector.postgresql_connection,
            )
            # TODO: Need a process to upload latest dataframe to S3.

    sectors.create_shares_outstanding_table()

    # Create or append stock history table for each ticker.
    for ticker in tickers.tickers.values():
        print(f"Start retrieve {ticker.ticker_symbol} stock history.")
        latest_date = ticker.get_stock_history_latest_date()  # Get latest date of stock history table.
        print(f"{ticker.ticker_symbol}, latest date: {latest_date}, today's date: {todays_date}")
        collect_stock_data = CollectDailyData(
            ticker.yfinance_ticker,
            todays_date=todays_date,
        )  # initialize CollectDailyData class.
        ticker.stock_history = collect_stock_data.get_ticker_history()  # retrieve stock history pd.DataFrame.
        if ticker.stock_history is not None:
            ticker.stock_history.columns = [column.lower() for column in ticker.stock_history.columns]  # Set column names to all lower-case letters.
            ticker.stock_history.index.name = ticker.stock_history.index.name.lower()  # Set date index to lower-case letters.
            stock_history = check_table_append_compatibility(latest_date, ticker.stock_history)  # Filter stock history to ensure no overlapping dates in postgreSQL table.
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
