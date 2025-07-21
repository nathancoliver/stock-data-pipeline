from pathlib import Path
from plotly.graph_objects import Figure, Scatter
from typing import List, Dict
import pandas as pd  # type: ignore
import sqlalchemy


from .definitions import SECTOR_SHARES_OUTSTANDING, DataTypes
from .functions import (
    get_s3_table,
    get_sql_table_latest_date,
    get_todays_date,
    initialize_table,
    set_table_primary_key,
)
from stock_data_pipeline import PostgreSQLConnection, S3Connection, create_directory
from .sector import Sector


class Sectors:
    def __init__(
        self,
        file_path: Path,
        postgresql_connection: PostgreSQLConnection,
        s3_connection: S3Connection,
    ):
        self.sectors: List[Sector] = []
        self.shares_outstanding: Dict[str, List[str | int]] = {
            "sector": [],
            "shares_outstanding": [],
        }
        self.sector_shares_outstanding_dtypes: Dict[str, sqlalchemy.types] = {"date": sqlalchemy.types.Date}
        self.sector_shares_outstanding_dtypes_strings: Dict[str, str] = {"date": DataTypes.DATE}
        self.sector_shares_directory = Path("sector_shares")
        self.sector_shares_outstanding_s3_file_name = f"{SECTOR_SHARES_OUTSTANDING}.csv"
        self.sector_shares_outstanding_s3_download_path = Path(
            self.sector_shares_directory,
            self.sector_shares_outstanding_s3_file_name,
        )
        self.postgresql_connection = postgresql_connection
        self.s3_connection = s3_connection

        create_directory(self.sector_shares_directory)

        with open(file_path, "r", encoding="utf-8") as file:
            for sector_ticker in file:
                sector = Sector(
                    sector_ticker.rstrip("\n"),
                    postgresql_connection=self.postgresql_connection,
                    s3_connection=s3_connection,
                    sector_shares_directory=self.sector_shares_directory,
                )
                self.sectors.append(sector)
                self.sector_shares_outstanding_dtypes.update({sector.sector_symbol: sqlalchemy.types.BigInteger})
                self.sector_shares_outstanding_dtypes_strings.update({sector.sector_symbol: DataTypes.BIGINT})

    def append_shares_outstanding_dict(self, sector: Sector, shares_outstanding: int):
        self.shares_outstanding["sector"].append(sector.sector_symbol)
        self.shares_outstanding["shares_outstanding"].append(shares_outstanding)

    def create_shares_outstanding_table(self):
        df_shares_outstanding = get_s3_table(
            self.s3_connection,
            s3_file_name=self.sector_shares_outstanding_s3_file_name,
            download_file_path=self.sector_shares_outstanding_s3_download_path,
        )
        initialize_table(
            table_name=SECTOR_SHARES_OUTSTANDING,
            data_types=self.sector_shares_outstanding_dtypes,
            data_types_strings=self.sector_shares_outstanding_dtypes_strings,
            postgresql_connection=self.postgresql_connection,
            data_frame=df_shares_outstanding,
        )
        # TODO: initilalize sql table (create table if exists ...)
        # TODO: create a row of shares outstanding and add row to existing sql tables
        todays_date = get_todays_date()
        latest_date = get_sql_table_latest_date(SECTOR_SHARES_OUTSTANDING, self.postgresql_connection.engine)
        shares_outstanding = {"date": [todays_date]}
        shares_outstanding_dtypes = {
            "date": sqlalchemy.DATE,
        }
        for sector in self.sectors:
            shares_outstanding.update({sector.sector_symbol: [sector.shares_outstanding]})
            shares_outstanding_dtypes.update(
                {
                    sector.sector_symbol: sqlalchemy.types.BigInteger,
                }
            )
        if latest_date is None:
            set_table_primary_key(SECTOR_SHARES_OUTSTANDING, "date", self.postgresql_connection)
        elif todays_date > latest_date:
            pd.DataFrame(shares_outstanding).set_index("date").to_sql(
                SECTOR_SHARES_OUTSTANDING,
                con=self.postgresql_connection.engine,
                if_exists="append",
                index=True,
                index_label="date",
                dtype=shares_outstanding_dtypes,
            )
        self.s3_connection.upload_sql_table(
            SECTOR_SHARES_OUTSTANDING,
            self.postgresql_connection,
        )

    def convert_shares_outstanding(self, shares_outstanding: str) -> int:
        magnitude = shares_outstanding.rstrip(" ")[-1].upper()
        value = float(shares_outstanding.rstrip(magnitude).strip(" "))
        if magnitude == "M":
            return int(value * 10**6)
        elif magnitude == "B":
            return int(value * 10**9)
        else:
            raise NameError(f"magnitude {magnitude} from shares_outstanding is not compatible with func convert_shares_outstanding. Consider editing func.")
