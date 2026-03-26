from app.services.bigquery import query_bigquery
from fastapi import APIRouter, HTTPException, Query, status
from google.cloud import bigquery

from ..services.conversion import convert_to_parquet
from ..services.gcs import upload_parquet_files
from ..services.bigquery import construct_external_tables
from ..utils.logger import get_logger

convertRouter = APIRouter(
    prefix="/convert",
    tags=["convert"]
)

TABLE: str = "`jbc-sales.jbc_sales_dataset.sales`"

@convertRouter.post("/", status_code=status.HTTP_200_OK)
async def convert_csvs(data_folder: str = ""):
    """
    Initializes the CSV to Parquet conversion process,
    sending the resulting files to GCS for analysis.
    """

    logger = get_logger(__name__)
    logger.info("Beginning convert_csvs endpoint execution")

    # a data folder needs to be supplied
    if data_folder == "":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST)

    try:
        convert_to_parquet(data_folder)

    except FileNotFoundError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="No CSV files found in given folder to convert."
                            )

    else:
        # call function to pass files to GCS here
        upload_parquet_files("jbc-sales-bucket", data_folder, "jbc", "sales")
        construct_external_tables()
        return {
            "status": "success" 
        }


queryRouter = APIRouter(
    prefix="/query",
    tags=["query"]
)

@queryRouter.get("/active", status_code=status.HTTP_200_OK)
async def most_active_customers(limit: int = 5, order_by: str = "DESC"):
    """
    Returns parameterized queries made through BigQuery
    as structured JSON payloads.
    """

    if limit > 10: limit = 10 # limit to 10 for readability of results
    if order_by not in ["ASC", "DESC"]: order_by = "DESC" # default to DESC if invalid input for order_by
        
    logger = get_logger(__name__)
    logger.info("Beginning most active query endpoint execution")
    
    query: str = f"""
        SELECT CustomerName, COUNT(CustomerName) AS TotalTransactions
        FROM {TABLE}
        GROUP BY CustomerName
        ORDER BY TotalTransactions {order_by}
        LIMIT @limit;
    """

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    return query_bigquery(query, job_config)

@queryRouter.get("/discounts", status_code=status.HTTP_200_OK)
async def discount_analysis(limit: int = 5, order_by: str = "DESC"):
    """
    Returns query results analyzing how much money in 
    discounts each store has given.
    """

    if limit > 10: limit = 10 # limit to 10 for readability of results
    if order_by not in ["ASC", "DESC"]: order_by = "DESC" # default to DESC if invalid input for order_by

    logger = get_logger(__name__)
    logger.info("Beginning discount analysis query endpoint execution")

    query: str = f"""
        SELECT
            StoreID,
            StoreLocation,
            SUM(UnitPrice * Quantity * DiscountPercent) AS TotalMoneyDiscounted
        FROM {TABLE}
        GROUP BY StoreID, StoreLocation
        ORDER BY TotalMoneyDiscounted {order_by}
        LIMIT @limit;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    return query_bigquery(query, job_config)

@queryRouter.get("/max_revenue_days", status_code=status.HTTP_200_OK)
async def max_revenue_days(limit: int = 5, order_by: str = "DESC"):
    """
    Returns query results analyzing the days with the
    highest revenue.
    """

    if limit > 10: limit = 10 # limit to 10 for readability of results
    if order_by not in ["ASC", "DESC"]: order_by = "DESC" # default to DESC if invalid input for order_by

    logger = get_logger(__name__)
    logger.info("Beginning highest revenue days query endpoint execution")

    query: str = f"""
        SELECT SUM(UnitPrice) as TotalDailyRevenue, Date
        FROM {TABLE}
        GROUP BY Date
        ORDER BY TotalDailyRevenue {order_by}
        LIMIT @limit;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    return query_bigquery(query, job_config)

@queryRouter.get("/top_products", status_code=status.HTTP_200_OK)
async def top_products(rank: int = 3, order_by: str = "DESC"):
    """
    Returns query results analyzing the top three
    products by revenue.
    """

    if rank > 5: rank = 5 # limit rank to 5 for readability of results
    if order_by not in ["ASC", "DESC"]: order_by = "DESC" # default to DESC if invalid input for order_by

    logger = get_logger(__name__)
    logger.info("Beginning top products query endpoint execution")

    query: str = f"""
        WITH ProductSales AS (
            SELECT 
                Category, 
                ProductName, 
                Count(ProductName) AS TimesProductBought,
                ROW_NUMBER() OVER(
                    PARTITION BY Category ORDER BY COUNT(ProductName) DESC) as Rank
            FROM {TABLE}
            GROUP BY Category, ProductName
        )

        SELECT 
        Category, 
        ProductName, 
        TimesProductBought,
        Rank
        FROM ProductSales
        WHERE Rank <= @rank
        ORDER BY Category, Rank;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("rank", "INT64", rank),
        ]
    )

    return query_bigquery(query, job_config)

@queryRouter.get("/worst_stores", status_code=status.HTTP_200_OK)
async def worst_stores(limit: int = 5, order_by: str = "ASC"):
    """
    Returns query results analyzing the stores with
    the lowest sales.
    """

    if limit > 10: limit = 10 # limit to 10 for readability of results
    if order_by not in ["ASC", "DESC"]: order_by = "ASC" # default to ASC if invalid input for order_by

    logger = get_logger(__name__)
    logger.info("Beginning worst stores query endpoint execution")

    query: str = f"""
        SELECT
            StoreID,
            SUM(TotalAmount) as TotalSales
        FROM {TABLE}
        GROUP BY StoreID
        ORDER BY TotalSales {order_by}
        LIMIT @limit;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    return query_bigquery(query, job_config)



@queryRouter.get("/orders_from_state", status_code=status.HTTP_200_OK)
async def orders_from_state(limit: int = 30, order_by: str = "DESC", state: str = "IL", order_total_above: int = 0):
    """
    Returns query results analyzing orders from
    a given state, above a certain order total (0 by default)
    """

    if limit > 1000: limit = 1000 # start getting issues on frontend client if sending more than 1000
    if order_by not in ["ASC", "DESC"]: order_by = "DESC" # default to ASC if invalid input for order_by

    logger = get_logger(__name__)
    logger.info("Beginning worst stores query endpoint execution")

    query: str = f"""
        SELECT
            TransactionID,
            State,
            UnitPrice,
            Quantity,
            TotalAmount
        FROM {TABLE}
        WHERE STATE = @state
        AND TotalAmount > @order_total_above
        ORDER BY TotalAmount {order_by}
        LIMIT @limit;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("state", "STRING", state),
            bigquery.ScalarQueryParameter("order_total_above", "INT64", order_total_above),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    return query_bigquery(query, job_config)
