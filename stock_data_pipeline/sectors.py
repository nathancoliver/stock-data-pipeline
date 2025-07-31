from re import sub

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


sector_color_map = {
    "xlb": "#8a98c5",
    "xlc": "#a967a6",
    "xle": "#fec839",
    "xlf": "#a9ce4f",
    "xli": "#8cc6e9",
    "xlk": "#92258b",
    "xlp": "#01acba",
    "xlre": "#a6011f",
    "xlu": "#ff972a",
    "xlv": "#01aeea",
    "xly": "#c6c953",
}


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
        value = float(sub(r"[,\s]", "", shares_outstanding.rstrip(magnitude)))
        if magnitude == "M":
            return int(value * 10**6)
        elif magnitude == "B":
            return int(value * 10**9)
        else:
            raise NameError(
                f"magnitude {magnitude} from shares_outstanding is not compatible with func convert_shares_outstanding. Consider editing func."
            )

    def plot_graphs(self, plot_directory: str | Path) -> None:
        figure = Figure()
        range_break = False
        range_break_dates: List[pd.DatetimeIndex] = []
        for sector in self.sectors:
            dates = list(sector.sector_history_df.index)
            sector_price = sector.sector_history_df[sector.sector_calculated_price_column_name]
            figure.add_trace(
                Scatter(
                    x=dates, y=sector_price, marker={"color": sector_color_map[sector.sector_symbol]}, mode="lines", name=sector.sector_symbol.upper()
                )
            )
            if not range_break:
                first_date = dates[0]
                last_date = dates[-1]
                date_range = pd.date_range(start=first_date, end=last_date, freq="D")
                x_min = str(date_range[0] - pd.DateOffset(days=1)).replace(" 00:00:00", "")
                x_max = str(date_range[-1]).replace(" 00:00:00", "")
                new_dates = [str(date).replace(" 00:00:00", "") for date in date_range]
                range_break_dates = [date for date in new_dates if date not in list(dates)]
                range_break = True
        figure = self.update_layout(figure, date_range_breaks=range_break_dates, x_min=x_min, x_max=x_max)
        figure.write_image(Path(plot_directory, "calculated_sector_prices.jpeg"), format="jpeg", scale=5, engine="kaleido")

    def update_layout(self, figure: Figure, date_range_breaks: List[pd.DatetimeIndex], x_min: str, x_max: str) -> Figure:
        Y_AXIS_TICK_FORMAT = ".4"
        GRID_LINES_COLOR = "rgba(128,128,128,0.3)"
        GRID_LINE_WIDTH = 1.5
        figure.update_layout(
            xaxis_title="Date",
            yaxis_title="Sector Price ($)",
            title="SPDR Sector Prices",
            title_x=0.5,
            showlegend=True,
            plot_bgcolor="white",
            font_color="black",
            yaxis_tickformat=Y_AXIS_TICK_FORMAT,
            yaxis={
                "zeroline": True,
                "zerolinecolor": GRID_LINES_COLOR,
                "zerolinewidth": GRID_LINE_WIDTH,
            },
        )
        figure.update_xaxes(
            linecolor="black",
            linewidth=GRID_LINE_WIDTH,
            mirror=True,
            fixedrange=True,
            ticks="outside",
            tickson="boundaries",
            tickwidth=GRID_LINE_WIDTH,
            tickcolor="black",
            rangebreaks=[{"values": date_range_breaks}],
            range=[x_min, x_max],
        )
        figure.update_yaxes(
            showline=True,
            linecolor="black",
            linewidth=GRID_LINE_WIDTH,
            mirror=True,
            showgrid=True,
            gridcolor=GRID_LINES_COLOR,
            gridwidth=GRID_LINE_WIDTH,
            fixedrange=False,
        )
        return figure
