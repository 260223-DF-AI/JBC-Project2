"""
Tests HTTP requests made to the API endpoints.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

app = FastAPI()

# @app.get("/")