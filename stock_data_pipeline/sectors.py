from pathlib import Path
from typing import List, Dict
from shutil import rmtree
import pandas as pd  # type: ignore
import sqlalchemy


from .chrome_driver import ChromeDriver
from .definitions import SECTOR_SHARES_OUTSTANDING, DataTypes
from .functions import (
    get_sql_table_latest_date,
    get_todays_date,
    initialize_table,
    set_table_primary_key,
)
from .postgresql_connection import PostgreSQLConnection
from .sector import Sector


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
        self.sector_shares_outstanding_dtypes: Dict[str, DataTypes] = {
            "date": DataTypes.DATE
        }
        self.postgresql_connection = postgresql_connection

        with open(file_path, "r", encoding="utf-8") as file:
            for sector_ticker in file:
                self.sectors.append(
                    Sector(
                        sector_ticker.rstrip("\n"),
                        chrome_driver=chrome_driver,
                        postgresql_connection=self.postgresql_connection,
                    )
                )
                self.sector_shares_outstanding_dtypes.update(
                    {sector_ticker: DataTypes.BIGINT}
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
            SECTOR_SHARES_OUTSTANDING, self.postgresql_connection.engine
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
                con=self.postgresql_connection.engine,
                if_exists="append",  # TODO: eventually this will need to be replaced with append
                index=True,
                index_label="date",
                dtype=shares_outstanding_dtypes,
            )
        if latest_date is None:
            set_table_primary_key(
                SECTOR_SHARES_OUTSTANDING, "date", self.postgresql_connection
            )

    def convert_shares_outstanding(self, shares_outstanding: str) -> int:
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
