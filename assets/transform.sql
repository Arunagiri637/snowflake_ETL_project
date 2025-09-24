-- 4_transform.sql
USE WAREHOUSE etl_wh;  -- Ensure the warehouse is active
USE DATABASE ecommerce_db;
USE SCHEMA ecommerce_db.analytics;
CREATE OR REPLACE TABLE sales_cleaned AS
SELECT
  InvoiceNo,
  StockCode,
  Description,
  Quantity,
  TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI') AS Date,  -- Specify the date format
  UnitPrice,
  CustomerID,
  Country,
  Quantity * UnitPrice AS TotalPrice
FROM ecommerce_db.raw.sales_raw
WHERE CustomerID IS NOT NULL
  AND Quantity > 0
  AND UnitPrice > 0;
CREATE OR REPLACE TABLE customers_dim AS
SELECT DISTINCT CustomerID AS customer_id, Country
FROM sales_cleaned;
CREATE OR REPLACE TABLE sales_fact AS
SELECT
  InvoiceNo AS invoice_no,
  CustomerID AS customer_id,
  Date,
  SUM(TotalPrice) AS OrderAmount
FROM sales_cleaned
GROUP BY InvoiceNo, CustomerID, Date;