from dotenv import load_dotenv
import os
import sys
from pathlib import Path

from google.cloud import bigquery, storage

import pandas as pd

def upload_parquet_files(
        bucket_name: str,
        local_folder: str,
        gcs_prefix: str = ""
    ) -> None:
    """Upload .parquet files to GCS Bucket"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    parquet_files = list(Path(local_folder).glob("**/*.parquet"))

    if not parquet_files:
        raise FileNotFoundError("No files in provided path")
    
    for file_path in parquet_files:
        relative_path = file_path.relative_to(local_folder)
        blob_name = f"{gcs_prefix}/{relative_path}".lstrip("/")

        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(file_path))
        # TODO Log and archive upload here, check for alreayd uploaded files using hash?


def main():
    """Connect to our GCS Bucket"""
    load_dotenv()
    creds_path = Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")).resolve()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds_path)
    proceed = input("Proceed? ").lower()
    if proceed not in ["y", "yes"]:
        sys.exit()


if __name__ == "__main__":
    main()
    parquets_folder_path = "" #TODO Replace with parquet file locations 
    upload_parquet_files("jbc-sales-bucket", parquets_folder_path)