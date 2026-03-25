from google.cloud import bigquery
from app.services.gcs import connect
from datetime import datetime
import logging
from google.cloud import storage
import pandas as pd
from app.utils.logger import get_logger

logger = get_logger(__name__)

def create_external_table(
        client: bigquery.Client = None,
        table: str = "transactions",
        schema: str = ""):
    """
    Create external table in BigQuery referencing our DataLake layer in GCS
    """
    if client is None:
        client = bigquery.Client(project=project_id)

    now = datetime.now()
    gcs_uri = f"gs://jbc-sales-bucket/jbc/stg_sales"

    # Build the SQL for creating external table, hive partitioning refers to the year/month fodler structure
    sql = f"""
CREATE OR REPLACE EXTERNAL TABLE `jbc-sales.jbc_sales_dataset.{table}` (
    TransactionID SERIAL PRIMARY KEY,
    Date DATE,
    StoreID VARCHAR(4),
    ProductID VARCHAR(4),
    Quantity INTEGER,
    UnitPrice DECIMAL(6, 2),
    DiscountPercent DECIMAL(5, 4),
    TaxAmount DECIMAL(6, 2),
    ShippingCost DECIMAL(6, 2),
    TotalAmount(7, 2),
    year SMALLINT,
    month TINYINT,
)
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['{gcs_uri}/year={now.year}/month={now.strftime('%m')}/{table}.parquet'],
  hive_partition_uri_prefix = '{gcs_uri}'
"""

    if schema:
        sql += f",\n  schema = '{schema}'"
    
    sql += "\n);"
    
    try:
        query_job = client.query(sql)
        query_job.result()  # Wait for the query to complete
        msg = f"External table {table} created successfully."
        # print(msg)
        logger.info(msg)
    except Exception as e:
        msg = f"Error creating external table {table}: {e}"
        # print(msg)
        logger.error(msg)

# def update_external_table(): # necessary?
#     """
    
#     """

def query_bigquery(client: bigquery.Client, sql: str, project_id) -> pd.DataFrame:
    """
    Send SQL query to our BigQuery data warehouse, return DataFrame
    """
    df = pd.DataFrame()

    try:
        query_job = client.query(sql)
        rows = query_job.result()
        df = rows.to_dataframe()
        msg = f"Query completed successfully"
        logger.info(msg)
    except Exception as e:
        msg = f"Error with query: {e}"
        logger.error(msg)

    return df

def construct_query(
        columns = "TransactionID", #str or list of strings
        join_tables = None, #

    ) -> str:
    """
    Take in certain parameters and construct a predefined DQL
    statement to be sent to BigQuery.

    Args:
    columns (string or string list): columns to fetch, if fetching non-transaction table columns must specific table name in column
    join_tables (tuple(str, str) or tuple list): tables to join, first item should be table name, second should be ids to join with transactions

    Returns:
    str: our full SQL statement to be sent to BigQuery
    """

    cols = ""
    if type(columns) == str:
        cols = columns
    elif type(columns) == list:
        for i, c in enumerate(columns):
            cols += c
            if i != len(columns) - 1:
                cols += ", "


    joins = ""
    if join_tables is not None:
        for table, id in join_tables:
            joins += f"JOIN {table} ON t.{id} = {table}.{id}\n"

    sql = f"""
SELECT {cols} FROM transactions as t
{joins}

"""
    

    return sql

# def get_dataset(project_id, dataset_id): # necessary?
#     """
    
#     """

# def partition_external_table():  # Handle year/month partitions
#     """
    
#     """


if __name__ == "__main__":
    connect()
    project_id = "jbc-sales"
    client = bigquery.Client(project=project_id)

    tables = ["transactions", "customers", "products", "stores", "dates"]

    for table in tables:
        create_external_table(client, table=table)
    