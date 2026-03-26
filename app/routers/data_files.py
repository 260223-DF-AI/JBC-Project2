import json
import pandas as pd
from app.services.bigquery import get_client
from fastapi import FastAPI, APIRouter, HTTPException, Query, status
from google.cloud import bigquery

from ..services.conversion import convert_to_parquet
from ..utils.logger import get_logger

convertRouter = APIRouter(
    prefix="/convert",
    tags=["convert"]
)

TABLE: str = "`jbc-sales.jbc_sales_dataset.sales`"

@convertRouter.post("/", status_code=status.HTTP_200_OK)
async def convert_csvs(data_folder: str = ""):
    """
    Initializes the CSV to Parquet conversion process,
    sending the resulting files to GCS for analysis.
    """

    logger = get_logger(__name__)
    logger.info("Beginning convert_csvs endpoint execution")

    # a data folder needs to be supplied
    if data_folder == "":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST)

    try:
        generated_file_paths: list[str] = convert_to_parquet(data_folder)

    except FileNotFoundError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="No CSV files found in given folder to convert."
                            )

    else:
        # call function to pass files to GCS here
        return {
            "files": generated_file_paths
        }


queryRouter = APIRouter(
    prefix="/query",
    tags=["query"]
)

@queryRouter.get("/active", status_code=status.HTTP_200_OK)
async def most_active_customers(limit: int):
    """
    Returns parameterized queries made through BigQuery
    as structured JSON payloads.
    """
        
    logger = get_logger(__name__)
    logger.info("Beginning most active query endpoint execution")
    
    query: str = f"""
SELECT CustomerName, COUNT(CustomerName) AS TotalTransactions
FROM {TABLE}
GROUP BY CustomerName
ORDER BY CustomerName DESC
LIMIT @limit;
"""

    client = get_client()
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("limit", "INT64", limit)
        ]
    )

    results: pd.DataFrame = client.query(query, job_config=job_config).to_dataframe()
    return results.to_json(orient="records")


