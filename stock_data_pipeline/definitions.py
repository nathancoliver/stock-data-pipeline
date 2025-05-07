from enum import Enum


# from sqlalchemy import BIGINT, DATE, INT, NUMERIC

from sqlalchemy.types import BigInteger, Date, Integer, Numeric


SECTOR_SHARES_OUTSTANDING = "sector_shares_outstanding"


class DataTypes:
    BIGINT = BigInteger
    DATE = Date
    INT = Integer
    NUMERIC_10_2 = Numeric(10, 2)


class SQLOperation(Enum):
    EXECUTE = "execute"
    COMMIT = "commit"
