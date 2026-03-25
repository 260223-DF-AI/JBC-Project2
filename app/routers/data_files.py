from fastapi import FastAPI, APIRouter, HTTPException, status
from google.cloud import bigquery

from ..services.conversion import convert_to_parquet

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
async def parameterized_query(params = None):
    """
    Returns parameterized queries made through BigQuery
    as structured JSON payloads.
    """
    if params is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="No query parameters provided."
                            )

    try:
        # call the query from bigquery here
        pass

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error executing query: {e}"
                            )