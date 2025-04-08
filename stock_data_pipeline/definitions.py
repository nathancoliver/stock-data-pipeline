from enum import Enum


SECTOR_SHARES_OUTSTANDING = "sector_shares_outstanding"


class DataTypes(Enum):
    BIGINT = "BIGINT"
    DATE = "DATE"
    INT = "INT"
    NUMERIC_10_2 = "NUMERIC(10, 2)"


class SQLOperation(Enum):
    EXECUTE = "execute"
    COMMIT = "commit"
