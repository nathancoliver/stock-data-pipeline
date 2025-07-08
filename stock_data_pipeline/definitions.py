from enum import Enum
from pathlib import Path


SECTOR_SHARES_OUTSTANDING = "sector_shares_outstanding"
STOCK_WEIGHT_DIRECTORY = Path("stock_weights")


class DataTypes:
    BIGINT = "BIGINT"
    DATE = "DATE"
    INT = "INT"
    NUMERIC_10_2 = "NUMERIC(10, 2)"


class SQLOperation(Enum):
    EXECUTE = "execute"
    COMMIT = "commit"


class TickerColumnType(Enum):
    PRICE = "price"
    SHARES = "shares"
