# TODO: create Tables class for function belows, and give inheritance to classes that need Tables class
# TODO: Tables class will probably need to inherit PostgreSQLConnection class. (need to confirm)


import datetime
import re
from typing import Dict


import pandas as pd  # type: ignore
import pandas_market_calendars as mcal
import sqlalchemy


from .definitions import DataTypes, SQLOperation
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


def convert_sql_data_type_into_string(data_types: Dict[str, DataTypes]) -> str:
    return ", ".join(
        [
            f"{column_name} {data_type.value}"
            for column_name, data_type in data_types.items()
        ]
    )


def get_market_day(date: datetime.datetime) -> bool:
    nyse = mcal.get_calendar("NYSE")
    market_days = nyse.valid_days(start_date=date, end_date=date)
    if len(market_days) > 0:  # TODO: make this more robust
        return True
    return False


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
    data_types: Dict[str, DataTypes],
    postgresql_connection: PostgreSQLConnection,
) -> None:
    dtypes_string = convert_sql_data_type_into_string(data_types)
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({dtypes_string})"
    postgresql_connection.execute_query(query, operation=SQLOperation.COMMIT)


def make_ticker_sql_compatible(name: str) -> str:
    return name.replace(".", "_").lower()


def make_ticker_yfinance_compatible(name: str) -> str:
    return re.sub(r"[._]", "-", name)


def set_table_primary_key(
    table_name: str, primary_key: str, postgresql_connection: PostgreSQLConnection
) -> None:
    postgresql_connection.set_primary_key(table_name, column=primary_key)
