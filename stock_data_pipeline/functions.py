# TODO: create Tables class for function belows, and give inheritance to classes that need Tables class
# TODO: Tables class will probably need to inherit PostgreSQLConnection class. (need to confirm)


import datetime
import os
from pathlib import Path
from shutil import rmtree
import re
from typing import Dict


import pandas as pd  # type: ignore
import pandas_market_calendars as mcal
import sqlalchemy


from .definitions import SQLOperation
from .postgresql_connection import PostgreSQLConnection
from .s3_connection import S3Connection


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


def convert_sql_data_type_into_string(data_types: Dict[str, str]) -> str:
    return ", ".join(
        [f"{column_name} {data_type}" for column_name, data_type in data_types.items()]
    )


def create_directory(directory_path: Path):
    if directory_path.exists():
        rmtree(directory_path)
    directory_path.mkdir(exist_ok=True)


def get_environment_variable(name: str, alternative_name: str | None = None) -> str:
    variable = os.getenv(name, alternative_name)
    if variable is None:
        raise TypeError(f"Environment variable {name} does not exist.")
    return variable


def get_market_day(date: datetime.datetime) -> bool:
    nyse = mcal.get_calendar("NYSE")
    market_days = nyse.valid_days(start_date=date, end_date=date)
    if len(market_days) > 0:  # TODO: make this more robust
        return True
    return False


def get_latest_date(df: pd.DataFrame, date_format: str) -> pd.DatetimeIndex | None:
    latest_date = (
        pd.to_datetime(df.index, format=date_format).sort_values(ascending=False)[0]
    ).to_pydatetime()  # TODO: figure out how to type hint this date
    return latest_date


def get_sql_table_latest_date(
    table_name: str, engine: sqlalchemy.engine.Engine
) -> datetime.datetime | None:

    try:
        df_shares_outstanding_shares_exists = pd.read_sql(table_name, engine)
        return get_latest_date(
            df_shares_outstanding_shares_exists, date_format="%Y-%m-%d"
        )
    except (FileNotFoundError, pd.errors.EmptyDataError):
        return None


def get_todays_date() -> datetime.datetime:
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


def initialize_table(
    table_name: str,
    data_types: Dict[str, sqlalchemy],
    data_types_strings: Dict[str, str],
    postgresql_connection: PostgreSQLConnection,
    data_frame: pd.DataFrame | None = None,
) -> None:
    dtypes_string = convert_sql_data_type_into_string(data_types_strings)
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({dtypes_string})"
    postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)
    if data_frame is not None:
        data_frame.to_sql(
            table_name,
            con=postgresql_connection.engine,
            if_exists="append",
            index=True,
            index_label="date",
            dtype=data_types,
        )
        set_table_primary_key(  # TODO: need to test when there are not sector shares csv files in S3 bucket
            table_name, "date", postgresql_connection
        )


def make_ticker_sql_compatible(name: str) -> str:
    return name.replace(".", "_").lower()


def make_ticker_yfinance_compatible(name: str) -> str:
    return re.sub(r"[._]", "-", name)


def set_table_primary_key(
    table_name: str, primary_key: str, postgresql_connection: PostgreSQLConnection
) -> None:
    postgresql_connection.set_primary_key(table_name, column=primary_key)
