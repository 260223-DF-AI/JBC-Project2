"""
Tests HTTP requests made to the API endpoints.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.server import app

# initialize the TestClient object with our FastAPI application
client = TestClient(app)

def test_convert_csvs():
    """
    Tests the /convert endpoint with a valid data folder containing .csv files.
    """

    response = client.post(
        "/convert/", 
        params={"data_folder": "tests/conversion_samples/"})

    assert response.status_code == 200
    assert "files" in response.json()