"""
Tests for the conversion service.
"""

import pytest
import os
import pandas as pd
from pandas.testing import assert_frame_equal

from app.services import conversion

def test_empty_parquets_exists():
    """
    Tests the parquets_exists function when the empty_samples folder is empty.
    """

    # should return False since no files exist within the empty_samples folder
    assert not conversion.parquets_exists("tests/empty_samples/")


def test_incorrect_file_type_parquets_exists():
    """
    Tests the parquets_exists function when the incorrect_samples folder contains incorrect file types.
    """

    # should return False since no parquet files exist within the incorrect_samples folder
    assert not conversion.parquets_exists("tests/incorrect_samples/")


def test_parquets_exists():
    """
    Tests the parquets_exists function when the samples folder contains parquet files amongst other file types.
    """

    # should return True despite there being .txt files within the samples folder
    assert conversion.parquets_exists("tests/samples/")


def test_convert_to_parquet():
    """
    Tests the convert_to_parquet function and checks if the data from the sample .csv files properly converts into .parquet files.
    """

    # run the conversion function on the sample .csv files
    generated_file_paths: list[str] = conversion.convert_to_parquet("tests/conversion_samples/")

    # after conversion, .parquet files should now exist
    assert conversion.parquets_exists("tests/conversion_samples/")

    # combine all original .csv files into a single DataFrame
    source_dir = "tests/conversion_samples/"
    csv_file_paths: list[str] = sorted(
        [
            os.path.join(source_dir, file_name)
            for file_name in os.listdir(source_dir)
            if file_name.endswith(".csv")
        ]
    )
    source_df: pd.DataFrame = pd.concat((pd.read_csv(path) for path in csv_file_paths), ignore_index=True)

    # define the columns that should be present in each generated .parquet file
    table_columns: dict[str, list[str]] = {
        "transactions": ["TransactionID", "Date", "StoreID", "ProductID", "Quantity", "UnitPrice", "DiscountPercent", "TaxAmount", "ShippingCost", "TotalAmount"],
        "stores": ["StoreID", "StoreLocation", "Region", "State"],
        "dates": ["Date"],
        "products": ["ProductID", "ProductName", "Category", "SubCategory"],
        "customers": ["CustomerID", "CustomerName", "Segment"],
    }

    # validate the generated .parquet file against source .csv data
    for table_name, columns in table_columns.items():
        parquet_path = os.path.join(source_dir, f"{table_name}.parquet")
        expected_df = source_df[columns].reset_index(drop=True)
        actual_df = pd.read_parquet(parquet_path).reset_index(drop=True)

        assert_frame_equal(expected_df, actual_df, check_dtype=False)

    # check if the conversion function still reports generated outputs
    assert len(generated_file_paths) == len(table_columns)