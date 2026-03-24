from google.cloud import bigquery
from app.services.gcs import connect


def create_external_table(
        client: bigquery.Client = None,
        project_id: str = "jbc-sales",
        dataset_id: str = "jbc_sales_dataset",
        table_name: str = "stg_sales",
        gcs_uri: str = "gs://jbc-sales-bucket/jbc/stg_sales",
        schema: str = ""):
    """
    Create external table in BigQuery referencing our DataLake layer in GCS
    """

    if client is None:
        client = bigquery.Client(project=project_id)
    
    # Build the SQL for creating external table
    sql = f"""
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.{table_name}`
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['{gcs_uri}/*'],
  hive_partition_uri_prefix = '{gcs_uri}'
"""
    
    if schema:
        sql += f",\n  schema = '{schema}'"
    
    sql += "\n);"
    
    try:
        query_job = client.query(sql)
        query_job.result()  # Wait for the query to complete
        print(f"External table {project_id}.{dataset_id}.{table_name} created successfully.")
    except Exception as e:
        print(f"Error creating external table: {e}")
        raise

def update_external_table():
    """
    
    """

def query_bigquery(sql, project_id):
    """
    
    """

def get_dataset(project_id, dataset_id):
    """
    
    """

def partition_external_table():  # Handle year/month partitions
    """
    
    """


if __name__ == "__main__":
    connect()
    project_id = "jbc-sales"
    client = bigquery.Client(project=project_id)
    create_external_table(client)

    