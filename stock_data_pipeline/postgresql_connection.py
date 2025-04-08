from typing import Dict
import psycopg2  # type: ignore
from sqlalchemy import create_engine


from .definitions import SQLOperation


class PostgreSQLConnection:

    def __init__(
        self, database_parameters: Dict[str, str | int], engine_parameters: str
    ):
        self.connection: psycopg2.extensions.connection = psycopg2.connect(
            **database_parameters
        )
        self.cursor: psycopg2.extensions.cursor = self.connection.cursor()
        self.engine = create_engine(engine_parameters)

    def execute_query(self, query, operation: SQLOperation, values=None):
        """Execute postgreSQL query."""

        if values:
            self.cursor.execute(query, values)  # Use values to parameterize the query
        else:
            self.cursor.execute(query)

        if operation == SQLOperation.COMMIT:
            self.connection.commit()
        elif operation == SQLOperation.EXECUTE:
            return self.cursor
        else:
            raise NameError(f"operation {SQLOperation} is not a valid input.")

    def set_primary_key(self, table_name: str, column: str) -> None:
        query = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column})"
        self.execute_query(query, SQLOperation.COMMIT)
