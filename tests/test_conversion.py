"""
Tests for the conversion service.
"""

import pytest
import os

from app.services import conversion

def test_empty_parquets_exists():
    """Tests the parquets_exists function when the empty_samples folder is empty"""

    # should return False since no files exist within the empty_samples folder
    assert not conversion.parquets_exists("tests/empty_samples/")


def test_incorrect_file_type_parquets_exists():
    """Tests the parquets_exists function when the incorrect_samples folder contains incorrect file types"""

    # should return False since no parquet files exist within the incorrect_samples folder
    assert not conversion.parquets_exists("tests/incorrect_samples/")


def test_parquets_exists():
    """Tests the parquets_exists function when the samples folder contains parquet files amongst other file types"""

    # should return True despite there being .txt files within the samples folder
    assert conversion.parquets_exists("tests/samples/")


def test_convert_to_parquet():
    """Tests the convert_to_parquet function and checks if the data from the sample .csv files properly converts into .parquet files"""

    # run the conversion function on our sample data
    conversion.convert_to_parquet("tests/conversion_samples/")

    # should create parquet files, so parquets_exists should return True
    assert conversion.parquets_exists("tests/conversion_samples/")