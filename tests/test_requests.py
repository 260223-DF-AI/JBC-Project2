"""
Tests HTTP requests made to the API endpoints.
"""

from fastapi.testclient import TestClient

from app.server import app

# initialize the TestClient object with our FastAPI application
client = TestClient(app)

def test_convert_base_case_success(monkeypatch):
    """Base case for POST /convert/."""
    from app.routers import data_files

    monkeypatch.setattr(data_files, "convert_to_parquet", lambda _folder: None)
    monkeypatch.setattr(data_files, "upload_parquet_files", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(data_files, "construct_external_tables", lambda: None)
    monkeypatch.setattr(data_files, "find_disk_savings_pct", lambda *_args, **_kwargs: 25.0)

    response = client.post("/convert/", params={"data_folder": "tests/conversion_samples/"})

    assert response.status_code == 200
    assert response.json() == {"status": "success"}


def test_convert_base_case_teapot():
    """Base case for POST /convert/ teapot response."""
    response = client.post("/convert/", params={"data_folder": "418"})
    assert response.status_code == 418


def test_query_active_base_case(monkeypatch):
    """Base case for GET /query/active."""
    from app.routers import data_files

    monkeypatch.setattr(
        data_files,
        "query_bigquery",
        lambda _query, _job_config: [{"CustomerName": "Alice", "TotalTransactions": 2}],
    )

    response = client.get("/query/active")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_query_discounts_base_case(monkeypatch):
    """Base case for GET /query/discounts."""
    from app.routers import data_files

    monkeypatch.setattr(
        data_files,
        "query_bigquery",
        lambda _query, _job_config: [{"StoreID": "S001", "TotalMoneyDiscounted": 9.5}],
    )

    response = client.get("/query/discounts")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_query_max_revenue_days_base_case(monkeypatch):
    """Base case for GET /query/max_revenue_days."""
    from app.routers import data_files

    monkeypatch.setattr(
        data_files,
        "query_bigquery",
        lambda _query, _job_config: [{"TotalDailyRevenue": 1000.0, "Date": "2025-01-01"}],
    )

    response = client.get("/query/max_revenue_days")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_query_top_products_base_case(monkeypatch):
    """Base case for GET /query/top_products."""
    from app.routers import data_files

    monkeypatch.setattr(
        data_files,
        "query_bigquery",
        lambda _query, _job_config: [{"Category": "Electronics", "ProductName": "Mouse", "Rank": 1}],
    )

    response = client.get("/query/top_products")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_query_worst_stores_base_case(monkeypatch):
    """Base case for GET /query/worst_stores."""
    from app.routers import data_files

    monkeypatch.setattr(
        data_files,
        "query_bigquery",
        lambda _query, _job_config: [{"StoreID": "S001", "TotalSales": 200.0}],
    )

    response = client.get("/query/worst_stores")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_query_orders_from_state_base_case(monkeypatch):
    """Base case for GET /query/orders_from_state."""
    from app.routers import data_files

    monkeypatch.setattr(
        data_files,
        "query_bigquery",
        lambda _query, _job_config: [{"TransactionID": 1, "State": "CA", "TotalAmount": 55.0}],
    )

    response = client.get("/query/orders_from_state", params={"state": "CA"})

    assert response.status_code == 200
    assert isinstance(response.json(), list)