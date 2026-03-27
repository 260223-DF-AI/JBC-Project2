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


def convert_to_parquet(data_folder: str, chunk_size: int = 10_000):
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
            
            # group chunks by actual (year, month) pairs present in rows
            for (year, month), partition_data in df.groupby(['year', 'month'], sort=False):
                partition_key = (year, month)
                partitions[partition_key].append(partition_data)
    
    # write one file per partition
    file_path: str = f"{data_folder}sales"
    os.makedirs(file_path, exist_ok=True)
    
    # Define PyArrow schema to ensure correct types for BigQuery
    schema_fields = []
    sample_combined_df = None
    for dfs in partitions.values():
        sample_combined_df = pd.concat(dfs, ignore_index=True)
        break
    
    if sample_combined_df is not None:
        for col_name in sample_combined_df.columns:
            col = sample_combined_df[col_name]
            if pd.api.types.is_integer_dtype(col):
                arrow_type = pa.int64()
            elif pd.api.types.is_float_dtype(col):
                arrow_type = pa.float64()
            elif pd.api.types.is_datetime64_any_dtype(col):
                arrow_type = pa.timestamp('us')
            else:
                arrow_type = pa.string()
            schema_fields.append(pa.field(col_name, arrow_type))
        schema = pa.schema(schema_fields)
    else:
        schema = None
    
    for (year, month), dfs in partitions.items():
        # combine all chunks for this partition
        combined_df = pd.concat(dfs, ignore_index=True)
        if combined_df.empty:
            logger.info(f"Skipping empty partition year={year}/month={month}")
            continue

        table = pa.Table.from_pandas(combined_df, schema=schema)
        
        # write to partitioned directory
        partition_dir = f"{file_path}/year={year}/month={month}"
        os.makedirs(partition_dir, exist_ok=True)
        pq.write_table(table, f"{partition_dir}/data.parquet")
        
        msg = f"Wrote {len(combined_df)} rows to year={year}/month={month}"
        logger.info(msg)


def find_disk_savings_pct(data_folder: str) -> float:
    """
    Return the disk savings, as a percentage, from converting CSVs to .parquets.
    """

    csv_total_size = 0
    parquet_total_size = 0
    
    if not os.path.exists(data_folder):
        raise FileNotFoundError(f"The directory {data_folder} does not exist.")

    # check folder and all subdirectories
    for root, _, files in os.walk(data_folder):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                csv_total_size += os.path.getsize(file_path)
            elif file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                parquet_total_size += os.path.getsize(file_path)

    # avoid DivideByZero
    if csv_total_size == 0:
        return 0.0

    savings = (csv_total_size - parquet_total_size) / csv_total_size
    
    # convert to percentage
    return savings * 100




if __name__ == "__main__":
    convert_to_parquet("data/")
    # print(f'{find_disk_savings_pct("data/"):.2f}%')