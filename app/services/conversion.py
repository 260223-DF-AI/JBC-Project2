import pandas as pd
import os
import sys
import logging

from ..validate import validate_df

logger = logging.getLogger(__name__)

def delete_existing_parquets(data_folder: str):
    """
    Having pre-generated parquet files from an earlier run of
    the project will result in error. This function removes
    pre-existing parquet files
    """

    # ensure directory exists
    if not os.path.exists(data_folder):
        print(f"Directory '{data_folder}' not found. Skipping cleanup.")
        return

    # delete parquet files
    with os.scandir(data_folder) as entries:
        for entry in entries:
            if entry.is_file() and entry.name.endswith('.parquet'):
                try:
                    os.remove(entry.path)
                    msg = f"Deleted: {entry.name}"
                    print(msg)
                    logger.info(msg)
                except OSError as e:
                    msg = f"Error deleting {entry.name}: {e}"
                    print(msg)
                    logger.error(msg)


def parquets_exists(data_folder: str):
    """
    Return True if Parquet files are created and in data folder,
    Return False otherwise
    """

    files: list[str] = os.listdir(data_folder)
    # there should be five 
    return len([1 for file in files if file.endswith("parquet")]) == 5


def convert_to_parquet(data_folder: str, chunk_size: int = 10_000) -> list[str]:
    """
    Efficiently convert CSV data to a parquet file
    """

    # delete pre-existing parquet files in data folder
    delete_existing_parquets(data_folder)

    # find source CSV files to operate on
    files: list[str] = os.listdir(data_folder)
    csvs: list[str] = [(data_folder + file) for file in files if file.endswith("csv")]

    # columns to keep for each table
    table_columns: dict[str, str] = {
        "transactions": ["TransactionID", "Date", "StoreID", "ProductID", "Quantity", "UnitPrice", "DiscountPercent", "TaxAmount", "ShippingCost", "TotalAmount"],
        "stores": ["StoreID", "StoreLocation", "Region", "State"],
        "dates": ["Date"],
        "products": ["ProductID", "ProductName", "Category", "SubCategory"],
        "customers": ["CustomerID", "CustomerName", "Segment"]
    }

    # read each csv in chunks, appending to parquet files as we go 
    for csv in csvs:
        csv_chunks = pd.read_csv(csv, chunksize=chunk_size)
        files_exist: bool = parquets_exists(data_folder)

        for chunk in csv_chunks:
            for table_name, columns in table_columns.items():
                df = chunk.filter(items=columns, axis=1)
                file_path: str = f"{data_folder}{table_name}.parquet"

                df = validate_df(df)
                
                # append or write to file depending on whether it already exists 
                df.to_parquet(
                    file_path,
                    engine='fastparquet',
                    append=files_exist
                )
        msg = f"Converted {csv} to parquet ({file_path})"
        print(msg)
        logger.info(msg)

    parquet_file_paths: list[str] = [f"{data_folder}{table_name}" for table_name in table_columns.keys()]
    return parquet_file_paths


if __name__ == "__main__":
    convert_to_parquet("data/")