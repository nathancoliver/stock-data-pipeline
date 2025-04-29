from pathlib import Path
from datetime import datetime
import pandas as pd
import yfinance as yf
import json
from typing import List


class CollectDailyData:

    def __init__(
        self,
        ticker,
        directory: Path = Path("."),
        save_as_feather: bool = False,
        latest_date: pd.DatetimeIndex | None = None,
    ):
        self.ticker = ticker
        self.latest_date = latest_date
        if directory == Path(".") and save_as_feather:
            raise NameError(
                "Cannot set save_as_feather to True and not specify a directory"
            )
        elif directory != Path(".") and not save_as_feather:
            raise NameError(
                "Cannot set save_as_feather to False and specify a directory"
            )
        if directory is not None:
            self.directory = directory
            file_name = f"{ticker}.feather"
            self.file_path = Path(self.directory, file_name)
        self.save_as_feather = save_as_feather
        end_date = datetime.now()
        if latest_date is None:
            start_date = end_date - pd.Timedelta(weeks=52 * 50)
            self.update = False
        else:
            start_date = self.latest_date + pd.Timedelta(days=1)
            self.update = True
        self.end_date = self.format_date_to_string(end_date)
        self.start_date = self.format_date_to_string(start_date)

    @staticmethod
    def format_date_to_string(date):
        """Format date to YFinance standards."""
        return date.strftime("%Y-%m-%d")

    def read_feather(self):
        "Read feather file."
        return pd.read_feather(self.file_path)

    def _download_ticker_history(self, start_date, end_date):
        """Download the entire price and volume history for a stock."""

        try:
            return (
                YFinance()
                .get_stock_data_single(
                    self.ticker,
                    "1d",
                    [start_date, end_date],
                )
                .drop(columns=["Dividends", "Stock Splits"])
            )

        except:
            print(
                f"Ticker {self.ticker} stock data does not exist from {start_date} to {end_date}"
            )

    def _update_ticker_history(self):
        """Update the price and volume history to include the latest day(s)."""

        start_date = self.format_date_to_string(self.latest_date + pd.Timedelta(days=1))
        latest_stock_history = self._download_ticker_history(start_date, self.end_date)
        if latest_stock_history is None:
            self.update = False
        return latest_stock_history

    @staticmethod
    def remove_time_zone_and_time_from_date(
        stock_history: pd.DataFrame,
    ) -> pd.DataFrame:
        """Remove time and timezone from stock_history Date index."""
        stock_history.index = pd.DatetimeIndex(stock_history.index.strftime("%Y-%m-%d"))
        return stock_history

    def get_ticker_history(self):
        """Update stock data (if file exists), or download full stock data (if file does not exist)."""

        if self.update:
            stock_history = self._update_ticker_history()
            if stock_history is not None:
                stock_history = self.remove_time_zone_and_time_from_date(stock_history)
        else:
            stock_history = self._download_ticker_history(
                self.end_date,
                self.end_date,  # TODO: Need to combine end_date and start_date into date.
            )
            if stock_history is not None:
                stock_history = self.remove_time_zone_and_time_from_date(stock_history)

        if stock_history is not None and self.save_as_feather:
            self.save_ticker_history_to_feather(stock_history)
        return stock_history

    def save_ticker_history_to_feather(self, stock_history: pd.DataFrame):
        """Save ticker history to feather file."""
        stock_history.to_feather(self.file_path)

    def check_history_update_status(self):
        return self.update


class YFinance:
    def get_ticker_data(self, path):
        with open(path, "r") as file:
            data = json.load(file)

        return data

    def get_ticker_list(self, path):
        with open(path, "r") as file:
            tickers = [ticker.strip() for ticker in file]

        return tickers

    def get_stock_data_multiple(self, tickers: List[int]) -> pd.DataFrame:

        dfs = []
        for ticker in tickers:
            stock = yf.Ticker(ticker)
            df_history = stock.history(period="max")
            dfs.append(df_history)
        return pd.concat(dfs, keys=tickers, axis=1)

    def get_stock_data_single(
        self, ticker: str, resolution: str, date_range: List[str]
    ) -> pd.DataFrame:
        start_year = date_range[0]
        end_year = date_range[1]
        stock = yf.Ticker(ticker)
        df_history = stock.history(interval=resolution, start=start_year, end=end_year)
        return df_history

    def get_stock_fine_resolution(
        self, ticker: str, resolution: str, date_range: List[str]
    ) -> pd.DataFrame:

        start_year = date_range[0]
        end_year = date_range[1]
        df_history = yf.download(
            ticker, period=resolution, start=start_year, end=end_year
        )
        return df_history

    def append_sma_column_to_dataframe(self, df: pd.DataFrame, sma: int):
        df[f"SMA {sma}"] = df["Close"].rolling(window=sma).mean()
        return df

    def append_dollar_volume_to_dataframe(self, df: pd.DataFrame):
        df["Dollar Volume ($)"] = df.apply(
            lambda x: (x["Close"] + x["Open"]) / 2 * x["Volume"],
            axis=1,
        )
        return df

    def append_gap_up_off_peak(self, df: pd.DataFrame):
        df_open = df["Open"]
        df_close = df["Open"].shift(1)
        df["Off Peak Price Change (%)"] = (df_open - df_close) / df_close * 100
        return df

    def append_gap_up_on_peak(self, df: pd.DataFrame):
        df_open = df["Open"]
        df_close = df["Close"]
        df["On Peak Price Change (%)"] = (df_close - df_open) / df_open * 100
        return df
