"""
Session-scoped fixtures that will generate test data and start a FastAPI
test client for the duration of the test session.
"""

import pytest
import os
import glob

# CSV content used by the conversion tests
_CSV_HEADER = (
    "TransactionID,Date,StoreID,StoreLocation,Region,State,"
    "CustomerID,CustomerName,Segment,ProductID,ProductName,"
    "Category,SubCategory,Quantity,UnitPrice,DiscountPercent,"
    "TaxAmount,ShippingCost,TotalAmount"
)
_CSV_BATCHES = [
    [
        "1,2025-01-10,S001,Downtown Boston,East,MA,C001,Alice Smith,Consumer,P101,Latitude 15,Electronics,Laptop,1,500.00,0.10,45.00,10.00,500.00",
        "2,2025-01-11,S002,Tech Ridge Austin,South,TX,C002,Bob Jones,Home Office,P102,iPhone 15 Pro,Electronics,Smartphone,2,799.99,0.05,76.00,8.00,1599.98",
    ],
    [
        "3,2025-02-01,S003,Belltown Seattle,West,WA,C003,Charlie Brown,Home Office,P103,Ergonomic Chair x200,Furniture,Seating,1,86.51,0.22,5.40,15.26,88.14",
        "4,2025-02-14,S004,Magnificent Mile Chicago,Midwest,IL,C004,Diana Prince,Consumer,P104,Ballpoint Multipack,Office Supplies,Pens,10,10.66,0.21,6.74,23.13,114.08",
    ],
    [
        "5,2025-03-05,S005,Colfax Denver,West,CO,C005,Eve Adams,Consumer,P105,Dell Quad display,Electronics,Monitor,3,822.60,0.13,286.26,25.62,2466.00",
        "6,2025-03-20,S006,Brickell Miami,South,FL,C006,Fiona Apple,Home Office,P106,Standard Copy Paper,Office Supplies,Paper,6,17.17,0.02,8.08,24.06,133.10",
    ],
    [
        "7,2025-04-15,S007,Times Square NY,East,NY,C007,George Costanza,Consumer,P107,Filing Cabinet 3-Tier,Furniture,Storage,7,405.12,0.06,213.26,18.20,2897.15",
        "8,2025-04-22,S008,SoMa San Francisco,West,CA,C008,Holden Caulfield,Consumer,P108,MX Master 3S,Electronics,Mouse,9,223.17,0.05,152.65,11.15,2071.90",
    ],
    [
        "9,2025-05-03,S001,Downtown Boston,East,MA,C001,Alice Smith,Consumer,P101,Latitude 15,Electronics,Laptop,2,500.00,0.10,90.00,10.00,1000.00",
        "10,2025-05-18,S003,Belltown Seattle,West,WA,C002,Bob Jones,Home Office,P102,iPhone 15 Pro,Electronics,Smartphone,1,799.99,0.05,38.00,8.00,799.99",
    ],
]


# Setup for test_conversion.py
def _write_csv(path: str, rows: list[str]) -> None:
    """
    Helper function to write a CSV file with given rows and a predefined header.
    """
    with open(path, "w", newline="") as f:
        f.write(_CSV_HEADER + "\n")
        for row in rows:
            f.write(row + "\n")


def _create_dir(path: str) -> None:
    """
    Helper function to create a directory with the given path if it
    exist yet.
    """
    os.makedirs(path, exist_ok=True)


@pytest.fixture(scope="session", autouse=True)
def create_test_fixtures():
    """
    Create all test sample directories and files before the test session.
    """

    # creates empty_samples folder with no files included
    _create_dir("tests/empty_samples")

    # creates incorrect_samples folder with .txt files included
    _create_dir("tests/incorrect_samples")
    for i in range(5):
        with open(f"tests/incorrect_samples/file_{i}.txt", "w") as f:
            f.write("")

    # creates samples folder with .txt and .parquet files included
    _create_dir("tests/samples")
    for i in range(5):
        with open(f"tests/samples/file_{i}.txt", "w") as f:
            f.write("")
    for i in range(5):
        with open(f"tests/samples/file_{i}.parquet", "w") as f:
            f.write("")

    # creates conversion_samples folder with .csv files containing dummy sales data
    _create_dir("tests/conversion_samples")
    for i, rows in enumerate(_CSV_BATCHES, start=1):
        path = f"tests/conversion_samples/dummy_sales_batch_{i}.csv"
        if not os.path.exists(path):
            _write_csv(path, rows)

    yield  # tests run here

    # remove all generated .parquet files after tests complete
    for parquet in glob.glob("tests/conversion_samples/*.parquet"):
        os.remove(parquet)
    for parquet in glob.glob("tests/samples/*.parquet"):
        os.remove(parquet)