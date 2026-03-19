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


def convert_to_parquet(chunk_size: int = 10_000):
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

        for chunk in csv_chunks:
            transactions_df: pd.DataFrame = chunk.filter(items=table_columns["transactions"], axis=1)
            stores_df: pd.DataFrame = chunk.filter(items=table_columns["stores"], axis=1)
            date_df: pd.DataFrame = chunk.filter(items=table_columns["dates"], axis=1)
            products_df: pd.DataFrame = chunk.filter(items=table_columns["products"], axis=1)
            customers_df: pd.DataFrame = chunk.filter(items=table_columns["customers"], axis=1)

            # initial write
            if not parquets_exists():
                transactions_df.to_parquet(data_folder + "transactions.parquet", engine='fastparquet')
                stores_df.to_parquet(data_folder + "stores.parquet", engine='fastparquet')
                date_df.to_parquet(data_folder + "date.parquet", engine='fastparquet')
                products_df.to_parquet(data_folder + "products.parquet", engine='fastparquet')
                customers_df.to_parquet(data_folder + "customers.parquet", engine='fastparquet')

            # append if file already made
            else:
                transactions_df.to_parquet(data_folder + "transactions.parquet", engine='fastparquet', append=True)
                stores_df.to_parquet(data_folder + "stores.parquet", engine='fastparquet', append=True)
                date_df.to_parquet(data_folder + "date.parquet", engine='fastparquet', append=True)
                products_df.to_parquet(data_folder + "products.parquet", engine='fastparquet', append=True)
                customers_df.to_parquet(data_folder + "customers.parquet", engine='fastparquet', append=True)


if __name__ == "__main__":
    convert_to_parquet()