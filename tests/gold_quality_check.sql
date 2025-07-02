/*
===============================================================================
Gold quality check: run some data quality check for the gold layer
===============================================================================
Script Purpose:
    This script run some test for the gold layer table
===============================================================================
*/


-- we care about the gender as it was sometimes not matching so we run test for it after the handling
-- expectation: ('Male', 'Female', 'n/a')
SELECT DISTINCT gender FROM gold.dim_customer;

-- for dimension products
SELECT * FROM gold.dim_products

-- for fact sales
SELECT * FROM gold.fact_sales;

-- foreign key Integrity
-- expectation: NONE
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL


SELECT
 * from information_schema.columns where table_schema LIKE '%gold%'




