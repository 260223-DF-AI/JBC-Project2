from google.cloud import bigquery, bigquery_storage
from app.services.gcs import fetch_creds
import pandas as pd
from app.utils.logger import get_logger
from typing_extensions import deprecated

logger = get_logger(__name__)

def get_client():
    """
    Return a BigQuery client object to be used elsewhere
    """

    fetch_creds()
    project_id = "jbc-sales"
    return bigquery.Client(project=project_id)


def create_external_table(
        client: bigquery.Client,
        table: str
    ):
    """
    Create external table in BigQuery referencing our DataLake layer in GCS

    Args:
        client (bigquery.Client): BigQuery client object to execute query
        table (str): name of the external table to create
    """

    gcs_uri = f"gs://jbc-sales-bucket/jbc/sales"

    # Build the SQL for creating external table, partition columns refers to hive partitioning (uri/year/month/.parquet)
    sql = f"""
CREATE OR REPLACE EXTERNAL TABLE `jbc-sales.jbc_sales_dataset.{table}`
WITH PARTITION COLUMNS (
    year STRING,
    month STRING
)
OPTIONS (
    format = 'PARQUET',
    uris = ['{gcs_uri}/*'],
    hive_partition_uri_prefix = '{gcs_uri}'
);
"""
    
    try:
        query_job = client.query(sql)
        query_job.result()  # Wait for the query to complete
        msg = f"External table {table} created successfully."
        logger.info(msg)
    except Exception as e:
        msg = f"Error creating external table {table}: {e}"
        logger.error(msg)

def construct_external_tables():
    """
    Create external tables in BigQuery for each parquet file in GCS
    """

    project_id = "jbc-sales"
    client = bigquery.Client(project=project_id)

    create_external_table(client, table="sales")

@deprecated("This function does not implement security features or protect against SQL injections.")
def construct_query(
        columns: tuple[str, str] = ("TransactionID", None),
        join_tables: tuple[str, str] = None,
        where: str = None,
        groups: str = None,
        having: str = None,
        order: tuple[str, str] = None,
        limit: int = 100
    ) -> str:
    """
    Take in certain parameters and construct a predefined DQL statement to be sent to BigQuery.

    Args:
        columns (tuple(str, str) or list(tuple)): columns to fetch, if fetching non-transaction table columns must specific table name in column. second value is optional alias for the column (can handle AGG fns as well)
        join_tables (tuple(str, str) or tuple list): tables to join, first item should be table name, second should be ids to join with transactions
        where (str): constraint ("col1 > 10"), will be appended to "WHERE"
        groups (str or list(str)): columns to group by, need to specify tablename 
        having (str) or list(str)): constraints on groups, need to specify tablename
        order (tuple(str, bool) or list(tuple)): tuples of columns to use in ordering and whether to use ASC (True) or DESC (False). Must include bool for every entry
        limit (int): Default 100, limit number of entries returned.

    Returns:
        str: our full SQL statement to be sent to BigQuery
    """

    # Select these columns with optional aliases
    cols = ""
    if type(columns) == tuple:
        cols = columns[0]
        if columns[1] is not None:
            cols += f" AS {columns[1]}"
    elif type(columns) == list:
        for i, col in enumerate(columns):
            cols += col[0]
            if col[1] is not None:
                cols += f" AS `{col[1]}`" #backticks for BigQuery quoted identifiers
            if i != len(columns) - 1:
                cols += ", "

    # leaving joins in in case of self join, but shouldn't use or need
    joins = ""
    if join_tables is not None:
        for table, id in join_tables:
            joins += f"JOIN jbc_sales_dataset.{table} as {table} ON sales.{id} = {table}.{id}\n"

    if where is not None:
        where = f"WHERE {where}"
    else:
        where = "" 

    # Group by these columns
    group_by = "GROUP BY "
    if type(groups) == str:
        group_by += groups
    elif type(groups) == list:
        for i, group in enumerate(groups):
            group_by += group
            if i != len(groups) - 1:
                group_by += ", "

    # Having these constraints on groups
    if having is not None:
        having = f"HAVING {having}"
    else:
        having = ""

    # Order by these columns with ASC or DESC (True for ASC, False for DESC)
    order_by = "ORDER BY "
    if type(order) == tuple:
        asc = "ASC" if order[1] else "DESC"
        order_by += f"{order[0]} {asc}"
    elif type(order) == list:
        for i, tup in enumerate(order):
            asc = "ASC" if tup[1] else "DESC"
            order_by += f"{tup[0]} {asc}"
            if i != len(order) - 1:
                order_by += ", "

    sql = f"""
SELECT {cols} FROM jbc_sales_dataset.sales as sales
{joins}
{where}
{group_by}
{having}
{order_by}
LIMIT {limit}
"""

    return sql

def query_bigquery(sql: str) -> pd.DataFrame:
    """
    Send SQL query to our BigQuery data warehouse, return DataFrame

    Args:
        sql (str): SQL statement to send to BigQuery
    
    Returns:
        pd.DataFrame: results of query
    """
    df = pd.DataFrame()

    project_id = "jbc-sales"
    client = bigquery.Client(project=project_id)
    storage_client = bigquery_storage.BigQueryReadClient()

    try:
        query_job = client.query(sql)
        rows = query_job.result()
        df = rows.to_dataframe(bqstorage_client=storage_client)
        msg = f"Query completed successfully"
        logger.info(msg)
    except Exception as e:
        msg = f"Error with query: {e}"
        logger.error(msg)

    return df


def demo():
    """
    Full pipeline demonstration: CSV -> Parquet -> GCS -> BigQuery -> DataFrame
    """

    # from app.services.conversion import convert_to_parquet
    # from app.services.gcs import upload_parquet_files

    # convert_to_parquet("data/")

    parquets_folder_path = "data/"
    # results = upload_parquet_files("jbc-sales-bucket", parquets_folder_path, "jbc", "stg_sales")


    sql = construct_query(
        columns = ("SUM(UnitPrice)", "Sum of Purchases over 1000"), #must use backtick for escapes
        # join_tables = None,
        where = "UnitPrice >= 1000",
        groups = ["CustomerID"],
        having = None,
        order = [("SUM(UnitPrice)", False)],
        limit = 10
    )
    print(sql)
    fetch_creds()


    # construct_external_tables()
    # client = bigquery.Client(project="jbc-sales")
    # create_external_table(client, "sales")

    df = pd.DataFrame()
    df = query_bigquery(sql)

    print(df)


if __name__ == "__main__":
    demo()