from fastapi import APIRouter

from ..services.conversion import convert_to_parquet

router = APIRouter(
    prefix="convert",
    tags=["convert"]
)


@router.post("/")
async def convert_csvs():
    """
    Initializes the CSV to Parquet conversion process,
    sending the resulting files to GCS for analysis.
    """

    generated_file_paths: list[str] = convert_to_parquet()

    # go on to use generated file paths to
    # send the parquet files to GCS

