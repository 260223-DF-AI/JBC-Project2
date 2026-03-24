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