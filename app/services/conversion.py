import pandas as pd
import os
import sys
import logging
from collections import defaultdict
import pyarrow as pa
import pyarrow.parquet as pq

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
    Efficiently convert CSV data to parquet files in chunks,
    writing one file per partition (year/month) using PyArrow.
    """

    # delete pre-existing parquet files in data folder
    delete_existing_parquets(data_folder)

    # find source CSV files to operate on
    files: list[str] = os.listdir(data_folder)
    csvs: list[str] = [(data_folder + file) for file in files if file.endswith("csv")]

    # collect data by partition to write one file per month
    partitions = defaultdict(list)
    
    # read each csv in chunks, validating and grouping by partition
    for csv in csvs:
        csv_chunks = pd.read_csv(csv, chunksize=chunk_size)
        for chunk in csv_chunks:
            df = validate_df(chunk)
            
            # group chunks by (year, month)
            for year in df['year'].unique():
                for month in df['month'].unique():
                    partition_key = (year, month)
                    partition_data = df[(df['year'] == year) & (df['month'] == month)]
                    partitions[partition_key].append(partition_data)
    
    # write one file per partition
    file_path: str = f"{data_folder}sales"
    os.makedirs(file_path, exist_ok=True)
    
    for (year, month), dfs in partitions.items():
        # combine all chunks for this partition
        combined_df = pd.concat(dfs, ignore_index=True)
        table = pa.Table.from_pandas(combined_df)
        
        # write to partitioned directory
        partition_dir = f"{file_path}/year={year}/month={month}"
        os.makedirs(partition_dir, exist_ok=True)
        pq.write_table(table, f"{partition_dir}/data.parquet")
        
        msg = f"Wrote {len(combined_df)} rows to year={year}/month={month}"
        logger.info(msg)

    parquet_file_paths: list[str] = [f"{data_folder}{table_name}.parquet" for table_name in ["transactions", "stores", "dates", "products", "customers"]]
    return parquet_file_paths


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