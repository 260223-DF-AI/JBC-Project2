from fastapi import APIRouter, HTTPException, status


from ..services.conversion import convert_to_parquet

router = APIRouter(
    prefix="/convert",
    tags=["convert"]
)


@router.post("/", status_code=status.HTTP_200_OK)
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
        pass


