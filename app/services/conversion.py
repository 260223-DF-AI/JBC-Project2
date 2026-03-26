import pandas as pd
import os
import sys
import logging

from ..utils.validate import validate_df

logger = logging.getLogger(__name__)

def delete_existing_parquets(data_folder: str):
    """
    Having pre-generated parquet files from an earlier run of
    the project will result in error. This function removes
    pre-existing parquet files
    """

    # ensure directory exists
    if not os.path.exists(data_folder):
        msg = f"Directory '{data_folder}' not found. Skipping cleanup."
        logger.warning(msg)
        return
    

    for root, dirs, files in os.walk(data_folder):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith(".parquet"):
                try:
                    os.remove(file_path)
                    msg = f"Deleted: {file}"
                    logger.info(msg)
                except OSError as e:
                    msg = f"Error deleting {file}: {e}"
                    logger.error(msg)


def convert_to_parquet(data_folder: str, chunk_size: int = 10_000):
    """
    Efficiently convert CSV data to a parquet file
    """

    # delete pre-existing parquet files in data folder
    delete_existing_parquets(data_folder)

    # find source CSV files to operate on
    files: list[str] = os.listdir(data_folder)
    csvs: list[str] = [(data_folder + file) for file in files if file.endswith("csv")]

    # read each csv in chunks, appending to parquet files as we go 
    for i, csv in enumerate(csvs):
        csv_chunks = pd.read_csv(csv, chunksize=chunk_size)
        # files_exist: bool = parquets_exists(data_folder)
        files_exist: bool = i > 0

        for chunk in csv_chunks:

            file_path: str = f"{data_folder}sales"
            
            df = validate_df(chunk)
            partition_cols = ["year", "month"]
            
            # append or write to file depending on whether it already exists 
            df.to_parquet(
                file_path,
                engine='fastparquet',
                index=False,
                partition_cols=partition_cols,
                append=files_exist
            )
        msg = f"Converted {csv} to parquet ({file_path})"
        logger.info(msg)


def find_disk_savings_pct(data_folder: str) -> float:
    """
    Return the disk savings, as a percentage, from converting CSVs to .parquets.
    """

    csv_total_size = 0
    parquet_total_size = 0
    
    if not os.path.exists(data_folder):
        raise FileNotFoundError(f"The directory {data_folder} does not exist.")

    # check folder
    with os.scandir(data_folder) as entries:
        for entry in entries:
            if entry.is_file():
                if entry.name.endswith('.csv'):
                    csv_total_size += entry.stat().st_size
                elif entry.name.endswith('.parquet'):
                    parquet_total_size += entry.stat().st_size

    # avoid DivideByZero
    if csv_total_size == 0:
        return 0.0

    savings = (csv_total_size - parquet_total_size) / csv_total_size
    
    # convert to percentage
    return savings * 100




if __name__ == "__main__":
    convert_to_parquet("data/")
    # print(f'{find_disk_savings_pct("data/"):.2f}%')