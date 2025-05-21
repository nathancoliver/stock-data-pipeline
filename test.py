import os
from pathlib import Path

import boto3

current_directory = os.getcwd()

AWS_USERNAME = os.getenv("AWS_USERNAME")
STOCK_DATA_PIPELINE_BUCKET_REGION_NAME = os.getenv(
    "STOCK_DATA_PIPELINE_BUCKET_REGION_NAME"
)
STOCK_DATA_PIPELINE_BUCKET_NAME = os.getenv("STOCK_DATA_PIPELINE_BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
file_name = "portfolio-holdings-xlb.csv"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=STOCK_DATA_PIPELINE_BUCKET_REGION_NAME,
    aws_account_id=AWS_USERNAME,
)
s3.upload_file(
    Path(os.getcwd(), "stock_weights", file_name),
    STOCK_DATA_PIPELINE_BUCKET_NAME,
    file_name,
)
s3.download_file(
    STOCK_DATA_PIPELINE_BUCKET_NAME,
    file_name,
    Path(current_directory, "download_file.csv"),
)
