from google.cloud import bigquery
from app.services.gcs import fetch_creds
from datetime import datetime
from google.cloud import storage
import pandas as pd
from app.utils.logger import get_logger

logger = get_logger(__name__)

def create_external_table(
        client: bigquery.Client,
        table: str,
        constraints: str
    ):
    """
    Create external table in BigQuery referencing our DataLake layer in GCS
    """

    now = datetime.now()
    gcs_uri = f"gs://jbc-sales-bucket/jbc/stg_sales"

    # Build the SQL for creating external table, hive partitioning refers to the year/month fodler structure
    sql = f"""
CREATE OR REPLACE EXTERNAL TABLE `jbc-sales.jbc_sales_dataset.{table}` (
{constraints}
)
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['{gcs_uri}/year={now.year}/month={now.strftime('%m')}/{table}.parquet'],
  hive_partition_uri_prefix = '{gcs_uri}'
);
"""
    
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

def construct_external_tables():
    """
    Create external tables in BigQuery for each parquet file in GCS
    """

    project_id = "jbc-sales"
    client = bigquery.Client(project=project_id)

    tables = [{
"name": "transactions",
"constraints": """
    TransactionID INT64,
    Date DATE,
    StoreID STRING,
    CustomerID STRING,
    ProductID STRING,
    Quantity INTEGER,
    UnitPrice DECIMAL,
    DiscountPercent DECIMAL,
    TaxAmount DECIMAL,
    ShippingCost DECIMAL,
    TotalAmount DECIMAL,
    year SMALLINT,
    month TINYINT,
"""}, {
"name": "customers",
"constraints": """
    CustomerID STRING,
    CustomerName STRING,
    Segment STRING,
    year SMALLINT,
    month TINYINT
"""}, {
"name": "products",
"constraints": """
    ProductID STRING,
    ProductName STRING,
    Category STRING,
    SubCategory STRING,
    year SMALLINT,
    month TINYINT
"""}, {
"name": "stores",
"constraints": """
    StoreID STRING,
    StoreLocation STRING,
    Region STRING,
    State STRING,
    year SMALLINT,
    month TINYINT
"""}, {
"name": "dates",
"constraints": """
    Date DATE,
    year SMALLINT,
    month TINYINT
"""}]

    for table in tables:
        create_external_table(client, table=table["name"], constraints=table["constraints"])


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

if __name__ == "__main__":

    # from app.services.conversion import convert_to_parquet
    # from app.services.gcs import upload_parquet_files

    # convert_to_parquet("data/")

    fetch_creds()

    # parquets_folder_path = "data/"
    # results = upload_parquet_files("jbc-sales-bucket", parquets_folder_path, "jbc", "stg_sales")

    construct_external_tables()
