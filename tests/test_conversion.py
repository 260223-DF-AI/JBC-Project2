"""
Tests for the conversion service.
"""

import os
from pathlib import Path
import pandas as pd
from pandas.testing import assert_frame_equal

from app.services import conversion
from app.utils.validate import validate_df

def test_convert_to_parquet():
    """
    Tests the convert_to_parquet function and checks if the data from the sample .csv files properly converts into .parquet files.
    """

    source_dir = Path(__file__).resolve().parent / "conversion_samples"
    data_folder = f"{source_dir}{os.sep}"

    # run the conversion function on the sample .csv files
    conversion.convert_to_parquet(data_folder)

    # combine all source .csv files
    csv_file_paths = sorted(source_dir.glob("*.csv"))
    source_df: pd.DataFrame = pd.concat(
        (validate_df(pd.read_csv(path)) for path in csv_file_paths),
        ignore_index=True,
    )

    sales_dir = source_dir / "sales"
    parquet_file_paths = sorted(sales_dir.glob("year=*/month=*/data.parquet"))
    assert parquet_file_paths, "No partitioned parquet files were generated."

    actual_df: pd.DataFrame = pd.concat(
        (pd.read_parquet(path) for path in parquet_file_paths),
        ignore_index=True,
    )

    # sort for deterministic comparison across partition write/read order.
    expected_sorted = source_df.sort_values(by=["TransactionID"]).reset_index(drop=True)
    actual_sorted = actual_df.sort_values(by=["TransactionID"]).reset_index(drop=True)

    assert_frame_equal(expected_sorted, actual_sorted, check_dtype=False)