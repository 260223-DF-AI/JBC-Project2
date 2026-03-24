"""
Tests for the conversion service.
"""

import pytest
import os

from app.services import conversion

def test_empty_parquets_exists():
    """Tests the parquets_exists function when the empty_samples folder is empty"""

    # checks if .tmp exists. if it does, remove it real quick for the test
    if os.path.exists("tests/empty_samples/.tmp"):
        os.remove("tests/empty_samples/.tmp")

    # should return False since no files exist within the empty_samples folder
    assert not conversion.parquets_exists("tests/empty_samples/")

    # re-add the .tmp file so the folder doesn't get deleted by git
    with open("tests/empty_samples/.tmp", "w") as f:
        f.write("")


def test_incorrect_file_type_parquets_exists():
    """Tests the parquets_exists function when the incorrect_samples folder contains incorrect file types"""

    # should return False since no parquet files exist within the incorrect_samples folder
    assert not conversion.parquets_exists("tests/incorrect_samples/")


def test_parquets_exists():
    """Tests the parquets_exists function when the samples folder contains parquet files amongst other file types"""

    # run the test if the parquet files already exist. most likely will not since we .gitignore .parquet files
    if conversion.parquets_exists("tests/samples/"):
        assert conversion.parquets_exists("tests/samples/")
    else:
        # create parquet files in the samples folder, then run the test
        for i in range(5):
            with open(f"tests/samples/file_{i}.parquet", "w") as f:
                f.write("")
    
        # should still return True despite there being other file types within the samples folder
        assert conversion.parquets_exists("tests/samples/")


# conversion.data_folder = "tests/csv_samples/"

# def test_convert_to_parquet():
#     """Tests the convert_to_parquet function and checks if the data from the CSV files properly converts"""

#     # run the conversion function on our sample data
#     conversion.convert_to_parquet()

#     # should create parquet files, so parquets_exists should return True
#     assert conversion.parquets_exists()

#     # remove the files we created so next test runs properly
#     filelist = [f for f in os.listdir(conversion.data_folder) if f.endswith(".parquet")]
#     for f in filelist:
#         os.remove(os.path.join(conversion.data_folder, f))