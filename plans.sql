-- get the customers with the most transactions
SELECT CustomerName, COUNT(CustomerName) AS TotalTransactions
FROM `jbc-sales.jbc_sales_dataset.stg_sales`
GROUP BY TotalTransactions
ORDER BY TotalTransactions DESC
LIMIT 10;

-- get the highest revenue days
SELECT SUM(UnitPrice) as TotalDailyRevenue, Date
FROM `jbc-sales.jbc_sales_dataset.stg_sales`
GROUP BY Date
ORDER BY TotalDailyRevenue DESC
LIMIT 10;

-- find the top three products of each category
WITH ProductSales AS (
    SELECT 
        Category, 
        ProductName, 
        Count(ProductName) AS TimesProductBought,
        ROW_NUMBER() OVER(
            PARTITION BY Category ORDER BY COUNT(ProductName) DESC) as Rank
    FROM `jbc-sales.jbc_sales_dataset.stg_sales`
    GROUP BY Category, ProductName
)

SELECT 
  Category, 
  ProductName, 
  TimesProductBought
FROM ProductSales
WHERE Rank <= 3
ORDER BY Category, TimesProductBought DESC;

-- lowest performing stores
SELECT
    StoreID,
    SUM(TotalAmount) as TotalSales
FROM `jbc-sales.jbc_sales_dataset.stg_sales`
GROUP BY StoreID
ORDER BY TotalSales ASC;

-- find how much money in discounts each store has given
SELECT
    StoreID,
    SUM(UnitPrice * Quantity * DiscountPercent) AS TotalMoneyDiscounted
FROM `jbc-sales.jbc_sales_dataset.stg_sales`
GROUP BY StoreID
ORDER BY TotalMoneyDiscounted DESC;