-- 5_orchestration.sql
CREATE OR REPLACE PROCEDURE ecommerce_db.analytics.run_sales_etl()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  CREATE OR REPLACE TABLE ecommerce_db.analytics.sales_cleaned AS
  SELECT
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI') AS Date,
    UnitPrice,
    CustomerID,
    Country,
    Quantity * UnitPrice AS TotalPrice
  FROM ecommerce_db.raw.sales_raw
  WHERE CustomerID IS NOT NULL
    AND Quantity > 0
    AND UnitPrice > 0;

  CREATE OR REPLACE TABLE ecommerce_db.analytics.customers_dim AS
  SELECT DISTINCT CustomerID AS customer_id, Country
  FROM ecommerce_db.analytics.sales_cleaned;

  CREATE OR REPLACE TABLE ecommerce_db.analytics.sales_fact AS
  SELECT
    InvoiceNo AS invoice_no,
    CustomerID AS customer_id,
    Date,
    SUM(TotalPrice) AS OrderAmount
  FROM ecommerce_db.analytics.sales_cleaned
  GROUP BY InvoiceNo, CustomerID, Date;

  RETURN 'ETL completed successfully at ' || CURRENT_TIMESTAMP();
END;
$$;

CREATE OR REPLACE TASK ecommerce_db.analytics.etl_sales_task
  WAREHOUSE = etl_wh
  SCHEDULE = 'USING CRON 0 0 * * * UTC'  -- Runs daily at 00:00 UTC (5:30 AM IST)
AS
  CALL ecommerce_db.analytics.run_sales_etl();

ALTER TASK ecommerce_db.analytics.etl_sales_task RESUME;