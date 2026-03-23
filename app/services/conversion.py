import pandas as pd
import os
import sys


data_folder: str = "data/"

def parquets_exists():
    """
    Return True if Parquet files are created and in data folder,
    Return False otherwise
    """

    files: list[str] = os.listdir(data_folder)
    # there should be five 
    return len([1 for file in files if file.endswith("parquet")]) == 5


def convert_to_parquet(chunk_size: int = 10_000) -> list[str]:
    """
    Efficiently convert CSV data to a parquet file
    """

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
        files_exist: bool = parquets_exists()

        for chunk in csv_chunks:
            for table_name, columns in table_columns.items():
                df = chunk.filter(items=columns, axis=1)
                file_path: str = f"{data_folder}{table_name}.parquet"
                
                # append or write to file depending on whether it already exists 
                df.to_parquet(
                    file_path,
                    engine='fastparquet',
                    append=files_exist
                )

    parquet_file_paths: list[str] = [f"{data_folder}{table_name}" for table_name in table_columns.keys()]
    return parquet_file_paths


if __name__ == "__main__":
    convert_to_parquet()