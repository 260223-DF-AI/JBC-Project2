"""
Tests for the conversion service.
"""

import pytest
import os

from app.services import conversion

# change the data_folder to our samples folder for testing
conversion.data_folder = "tests/samples/"

def test_empty_parquets_exists():
    """Tests the parquets_exists function when the samples folder is empty"""

    # should return False if no parquet files exist
    assert not conversion.parquets_exists()


def test_incorrect_file_type_parquets_exists():
    """Tests the parquets_exists function when only incorrect file types exist"""

    # create incorrect file types in the samples folder
    for i in range(5):
        with open(f"{conversion.data_folder}/file_{i}.txt", "w") as f:
            f.write("")

    # should return False since no parquet files exist
    assert not conversion.parquets_exists()

    # remove the incorrect files we created so next test runs properly
    filelist = [f for f in os.listdir(conversion.data_folder) if f.endswith(".txt")]
    for f in filelist:
        os.remove(os.path.join(conversion.data_folder, f))


def test_parquets_exists():
    """Tests the parquets_exists function when parquet files exist"""

    # create parquet files in the samples folder
    for i in range(5):
        with open(f"{conversion.data_folder}/file_{i}.parquet", "w") as f:
            f.write("")

    # returns True since the parquet files were created
    assert conversion.parquets_exists()

    # create incorrect file types in the samples folder. should not affect results
    for i in range(5):
        with open(f"{conversion.data_folder}/file_{i}.txt", "w") as f:
            f.write("")
    
    # should still return True since the parquet files exist
    assert conversion.parquets_exists()

    # remove the files we created so next test runs properly
    filelist = [f for f in os.listdir(conversion.data_folder) if f.endswith(".parquet") or f.endswith(".txt")]
    for f in filelist:
        os.remove(os.path.join(conversion.data_folder, f))

conversion.data_folder = "tests/csv_samples/"

def test_convert_to_parquet():
    """Tests the convert_to_parquet function and checks if the data from the CSV files properly converts"""

    # run the conversion function on our sample data
    conversion.convert_to_parquet()

    # should create parquet files, so parquets_exists should return True
    assert conversion.parquets_exists()

    # remove the files we created so next test runs properly
    filelist = [f for f in os.listdir(conversion.data_folder) if f.endswith(".parquet")]
    for f in filelist:
        os.remove(os.path.join(conversion.data_folder, f))