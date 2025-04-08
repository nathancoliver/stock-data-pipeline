from typing import Dict


from .ticker import Ticker


class Tickers:

    def __init__(self):
        self.tickers: Dict[str, Ticker] = {}

    def add_ticker(self, ticker_symbol: str, ticker_object):
        if ticker_symbol not in self.tickers:
            self.tickers.update({ticker_symbol: ticker_object})
