import os
from pathlib import Path
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from stock_data_pipeline import PostgreSQLConnection


class S3Connection:

    def __init__(
        self,
        stock_weight_directory: Path,
        data_directory: Path,
        AWS_ACCESS_KEY: str,
        AWS_SECRET_ACCESS_KEY: str,
        STOCK_DATA_PIPELINE_BUCKET_NAME: str,
        STOCK_DATA_PIPELINE_BUCKET_REGION_NAME: str,
        AWS_USERNAME: str,
    ):
        self.stock_weight_directory = stock_weight_directory
        self.data_directory = data_directory
        self.AWS_ACCESS_KEY = AWS_ACCESS_KEY
        self.AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY
        self.STOCK_DATA_PIPELINE_BUCKET_NAME = STOCK_DATA_PIPELINE_BUCKET_NAME
        self.STOCK_DATA_PIPELINE_BUCKET_REGION_NAME = (
            STOCK_DATA_PIPELINE_BUCKET_REGION_NAME
        )
        self.AWS_USERNAME = AWS_USERNAME
        self.s3_connection = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=STOCK_DATA_PIPELINE_BUCKET_REGION_NAME,
            aws_account_id=AWS_USERNAME,
        )
        self.current_working_directory = os.getcwd()

    def upload_sql_table(
        self,
        table_name: str,
        postgresql_connection: "PostgreSQLConnection",
    ):
        csv_file_name = f"{table_name}.csv"
        csv_file_path = Path(self.data_directory, csv_file_name)
        postgresql_connection.save_sql_table_to_csv(table_name, csv_file_path)
        self.s3_connection.upload_file(
            csv_file_path,
            self.STOCK_DATA_PIPELINE_BUCKET_NAME,
            csv_file_name,
        )

    def download_file(self, s3_file_name: str, download_file_path: Path):
        self.s3_connection.download_file(
            self.STOCK_DATA_PIPELINE_BUCKET_NAME,
            s3_file_name,
            Path(self.current_working_directory, download_file_path),
        )
