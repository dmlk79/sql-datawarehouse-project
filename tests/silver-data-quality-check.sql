/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================

/* 
===============================
-- For TABLE crm_cust_info:
=================================
*/
-- check For nulls or duplicates in primary key
-- expectation: no result

SELECT
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--check for unwanted spaces
-- expectation: no result
SELECT 	
	cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT 	
	cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- data standardization & consistency (for gender... for instance)
-- cst_gndr
	-- expectation: ('Male', 'Female', 'n/a')
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

-- cst_marital_status
	-- expectation: ('')
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info


-- quick look:
SELECT * FROM silver.crm_cust_info;


/* 
===============================
-- For TABLE crm_prd_info:
=================================
*/
-- checks duplicates or nulls
-- expectation: no result --> SUCCESSFULL
SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL  

-- derive new columns from prd_key as it contains the category_id( first 5 caracters)
SELECT 
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id
FROM silver.crm_prd_info

-- we have only 1 category which is not in the erp_px_cat_g1v2 : 
SELECT 
cat_id,
COUNT(*) 
FROM
(SELECT 
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id
FROM silver.crm_prd_info
GROUP BY cat_id

-- check for unwanted spaces:
-- expectation: no results --> SUCCESSFUL
SELECT	
	prd_nm
FROM silver.crm_prd_info 
WHERE prd_nm != TRIM(prd_nm)

-- check for nulls or negative price (prd_cost)
-- expectation: no results --> 2 nulls that we have so replace it with 0 if business allows it
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


-- data standardization & consistency 
-- to replace after asking to the business expert we got answers
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- check for date consistency
SELECT *
FROM silver.crm_prd_info 
WHERE prd_end_dt < prd_start_dt


/* ===============================
-- For TABLE crm_sales_details:
=================================
*/
-- check for unwanted spaces:
-- expected result: NONE --> SUCCESSFULL
SELECT
	* 
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- check weither all product key are in crm_prd_info
-- expectation: more the bigger more the better 
SELECT
	*
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

-- the same for crm_cust_info
-- expectation: more the bigger more the better 
SELECT
	*
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- check for date inconsistency

SELECT
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM silver.crm_sales_details

/* we have integer for date so let's cast it but before we should ensure that */
-- check for negative or 0 number
SELECT
sls_order_dt,
COUNT(*) AS num_Zero_date
FROM silver.crm_sales_details
WHERE sls_order_dt <=0
GROUP BY sls_order_dt

SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <=0 OR LENGTH(sls_order_dt::TEXT) != 8

-- 17 date with zero --> replace with NULL at the cleansing
-- check for outliers by validating  the boundaries of the data range
SELECT
  sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > DATE '2050-01-01'
   OR sls_order_dt < DATE '1990-01-01';

-- check for invalid date orders
-- expectation: NONE --> SUCCESSFUL
SELECT
	*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- check data consistency : between sales, quantity and price
-- >> sales= quantity * price
-- >> values must not be NULL, zeros or negative
SELECT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0


/* ===============================
-- For TABLE erp_cust_az12:
=================================*/


-- cid contains silver.erp_cust_az12.cst_key but some of them start with 'NAS' --> remove 'NAS' using case as others may not contain it
SELECT 
	*
FROM silver.erp_cust_az12
WHERE cid LIKE 'NAS%'

-- check for date consistency
--expectation: NONE
SELECT
	bdate
FROM silver.erp_cust_az12
WHERE bdate > NOW()

-- check for data consistency for gender
-- expectation: Male , Female and 'n/a'(for none identified gender) 
SELECT DISTINCT
	gen
FROM silver.erp_cust_az12

/* ===============================
-- For TABLE erp_loc_a101:
=================================*/

-- CHECK if cid matches cst_key of bronze.crm_cust_info(it should not contain '-')
SELECT
	cid
FROM silver.erp_loc_a101
WHERE cid LIKE '%-%'

-- check for unwanted spaces and mispelling cntry
-- EXPECTATION: not misspelled country
SELECT DISTINCT
	cntry 
FROM silver.erp_loc_a101


/* ===============================
-- For TABLE erp_px_cat_g1v2:
=================================*/

-- check for unwanted spaces for cat
-- expected results: NONE
SELECT	
	* 
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)

-- check for unwanted spaces for subcat
-- expected results: NONE
SELECT	
	* 
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)

-- data standardization & consistency for 
-- expected results: ('Yes','No')
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2




