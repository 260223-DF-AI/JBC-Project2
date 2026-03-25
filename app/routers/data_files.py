import json
import pandas as pd
from app.services.bigquery import query_bigquery
from fastapi import FastAPI, APIRouter, HTTPException, Query, status
from google.cloud import bigquery

from ..services.conversion import convert_to_parquet
from ..utils.logger import get_logger

convertRouter = APIRouter(
    prefix="/convert",
    tags=["convert"]
)

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

@queryRouter.get("/", status_code=status.HTTP_200_OK)
async def parameterized_query(
        sql: str = Query(..., description="SQL query to execute; use @param_name for named parameters."),
        params: str = Query(None, description="Optional JSON-encoded object of named parameter values, e.g. {\"year\": \"2024\"}")
    ):
    """
    Returns parameterized queries made through BigQuery
    as structured JSON payloads.
    """
    parsed_params: dict | None = None
    if params is not None:
        try:
            parsed_params = json.loads(params)
        except json.JSONDecodeError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="params must be a valid JSON object string."
                                )
        
    logger = get_logger(__name__)
    logger.info("Beginning query endpoint execution")
    try:
        rows = query_bigquery(sql, parsed_params)
        return {"data": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error executing query: {e}"
                            )