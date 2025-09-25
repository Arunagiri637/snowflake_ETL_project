-- 6_validation.sql
SELECT COUNT(*) AS NullCustomers FROM ecommerce_db.analytics.sales_cleaned WHERE CustomerID IS NULL;
SELECT COUNT(*) AS InvalidTotals FROM ecommerce_db.analytics.sales_cleaned WHERE TotalPrice <= 0;
SELECT InvoiceNo, COUNT(*) AS cnt FROM ecommerce_db.analytics.sales_fact GROUP BY InvoiceNo HAVING cnt > 1;