from re import sub

from pathlib import Path
from plotly.graph_objects import Figure, Scatter
from typing import Dict, List
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
    "xlb": "#a6cee3",
    "xlc": "#1f78b4",
    "xle": "#b2df8a",
    "xlf": "#33a02c",
    "xli": "#fb9a99",
    "xlk": "#e31a1c",
    "xlp": "#fdbf6f",
    "xlre": "#ff7f00",
    "xlu": "#cab2d6",
    "xlv": "#6a3d9a",
    "xly": "#b15928",
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
        x_min: str = ""
        x_max: str = ""
        for sector in self.sectors:
            dates = list(sector.sector_history_df.index)
            sector_prices = sector.sector_history_df[sector.sector_calculated_price_column_name]
            figure.add_trace(
                Scatter(
                    x=dates,
                    y=sector_prices,
                    marker={"color": sector_color_map[sector.sector_symbol]},
                    mode="lines",
                    name=sector.sector_symbol.upper(),
                )
            )
            if not range_break:
                date_range = self._add_date_range(dates)
                x_min, x_max = self._get_date_limits(date_range)
                range_break_dates = self._add_range_break_dates(dates, date_range)

                x_min = date_range[0] - pd.DateOffset(days=1)
                x_max = date_range[-1] + pd.DateOffset(days=1)
                range_break_dates = [date for date in date_range if date not in dates]

                range_break = True
        figure = self.update_layout(
            figure, date_range_breaks=range_break_dates, x_min=x_min, x_max=x_max, title="SPDR Sector Prices", y_axis_title="Sector Price ($)"
        )
        figure.write_image(Path(plot_directory, "calculated_sector_prices.jpeg"), format="jpeg", scale=5, engine="kaleido")

    def plot_percent_difference_graphs(self, plot_directory: str | Path, days: int) -> None:
        figure = Figure()
        range_break = False
        range_break_dates: List[pd.DatetimeIndex] = []
        x_min: str = ""
        x_max: str = ""
        for sector in self.sectors:
            if len(sector.sector_history_df) < days:
                continue
            dates = list(sector.sector_history_df.index[-days:])
            sector_prices = sector.sector_history_df[sector.sector_calculated_price_column_name][-days:]
            start_sector_price = sector_prices[0]
            if start_sector_price is None:
                continue
            percent_sector_prices = [(sector_price - start_sector_price) * 100 / start_sector_price for sector_price in sector_prices]
            figure.add_trace(
                Scatter(
                    x=dates,
                    y=percent_sector_prices,
                    marker={"color": sector_color_map[sector.sector_symbol]},
                    mode="lines",
                    name=sector.sector_symbol.upper(),
                )
            )
            if not range_break:
                date_range = self._add_date_range(dates)
                x_min, x_max = self._get_date_limits(date_range)
                range_break_dates = self._add_range_break_dates(dates, date_range)
                range_break = True
        figure = self.update_layout(
            figure,
            date_range_breaks=range_break_dates,
            x_min=x_min,
            x_max=x_max,
            title=f"SPDR Sectors {days}-Day Relative Price Movement",
            y_axis_title="Percent Change (%)",
        )
        figure.write_image(Path(plot_directory, f"percent_sector_prices_{days}_days.jpeg"), format="jpeg", scale=5, engine="kaleido")

    @staticmethod
    def _add_date_range(dates: pd.DatetimeIndex):
        first_date = dates[0]
        last_date = dates[-1]
        return pd.date_range(start=first_date, end=last_date, freq="D")

    @staticmethod
    def _get_date_limits(date_range: pd.DatetimeIndex) -> tuple[pd.Timestamp, pd.Timestamp]:
        x_min = date_range[0] - pd.DateOffset(days=1)
        x_max = date_range[-1] + pd.DateOffset(days=1)
        return x_min, x_max

    @staticmethod
    def _add_range_break_dates(dates: pd.DatetimeIndex, date_range: pd.DatetimeIndex) -> pd.DatetimeIndex:
        return [date for date in date_range if date not in dates]

    def update_layout(
        self, figure: Figure, date_range_breaks: List[pd.DatetimeIndex], x_min: pd.Timestamp, x_max: pd.Timestamp, title: str, y_axis_title: str
    ) -> Figure:
        Y_AXIS_TICK_FORMAT = ".4"
        GRID_LINES_COLOR = "rgba(128,128,128,0.3)"
        GRID_LINE_WIDTH = 1.5
        figure.update_layout(
            xaxis_title="Date",
            yaxis_title=y_axis_title,
            title=title,
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
