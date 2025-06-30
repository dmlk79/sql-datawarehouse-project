/*
============================================================================================
Checking and Cleaning Script: checking data quality and cleaning
============================================================================================
Script Purpose:
    This script is divided into 2 parts:
		 the first part (DATA quality check...) is a set of verification/data
			checking we do in order to notes the adequate transformation for the silver
		 the second one (DATA cleansing SCRIPT) is the script used to clean the bronze data 
			based on the analyze in the (DATA quality check...) script

============================================================================================
*/








/*
==============================================================
                      DATA quality check...:
============================================================
*/

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
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--check for unwanted spaces
-- expectation: no result
SELECT 	
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT 	
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- data standardization & consistency (for gender... for instance)
-- cst_gndr
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

-- cst_marital_status
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info


/* 
===============================
-- For TABLE crm_prd_info:
=================================
*/
-- display all columns 
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'bronze'  
  AND table_name = 'crm_prd_info';

-- checks duplicates or nulls
-- expectation: no result --> SUCCESSFULL
SELECT 
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL  

-- derive new columns from prd_key as it contains the category_id( first 5 caracters)
SELECT 
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id
FROM bronze.crm_prd_info

-- we have only 1 category which is not in the erp_px_cat_g1v2 : 
SELECT 
cat_id,
COUNT(*) 
FROM
(SELECT 
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5), '-', '_') NOT IN (SELECT id FROM bronze.erp_px_cat_g1v2))
GROUP BY cat_id

-- check for unwanted spaces:
-- expectation: no results --> SUCCESSFUL
SELECT	
	prd_nm
FROM bronze.crm_prd_info 
WHERE prd_nm != TRIM(prd_nm)

-- check for nulls or negative price (prd_cost)
-- expectation: no results --> 2 nulls that we have so replace it with 0 if business allows it
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


-- data standardization & consistency 
-- to replace after asking to the business expert we got answers
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- check for date consistency
SELECT *
FROM bronze.crm_prd_info 
WHERE prd_end_dt < prd_start_dt


/* ===============================
-- For TABLE crm_sales_details:
=================================
*/

-- check for unwanted spaces:
-- expected result: NONE --> SUCCESSFULL
SELECT
	* 
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- check weither all product key are in crm_prd_info
-- expectation: more the bigger more the better 
SELECT
	*
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

-- the same for crm_cust_info
-- expectation: more the bigger more the better 
SELECT
	*
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- check for date inconsistency

SELECT
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details

/* we have integer for date so let's cast it but before we should ensure that */
-- check for negative or 0 number
SELECT
sls_order_dt,
COUNT(*) AS num_Zero_date
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0
GROUP BY sls_order_dt

SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 OR LENGTH(sls_order_dt::TEXT) != 8

-- 17 date with zero --> replace with NULL at the cleansing
-- check for outliers by validating  the boundaries of the data range
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > 20500101 OR sls_order_dt < 19900101
-- check for invalid date orders
-- expectation: NONE --> SUCCESSFUL
SELECT
	*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- check data consistency : between sales, quantity and price
-- >> sales= quantity * price
-- >> values must not be NULL, zeros or negative
SELECT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0


/* ===============================
-- For TABLE erp_cust_az12:
=================================*/
SELECT 	
	cid,
	bdate,
	gen
FROM bronze.erp_cust_az12;

-- cid contains bronze.erp_cust_az12.cst_key but some of them start with 'NAS' --> remove 'NAS' using case as others may not contain it
SELECT 
	*
FROM bronze.erp_cust_az12
WHERE cid LIKE 'NAS%'

-- check for date consistency
--expectation: NONE
SELECT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate > NOW()

-- check for data consistency for gender
-- expectation: Male , Female and 'n/a'(for none identified gender) 
SELECT DISTINCT
	gen
FROM bronze.erp_cust_az12


/* ===============================
-- For TABLE erp_loc_a101:
=================================*/

SELECT
	cid,
	cntry
FROM bronze.erp_loc_a101

-- cid contains cst_key from bronze.crm_cust_info but it has '-' that we should remove further in the cleansing

-- check for unwanted spaces and mispelling cntry
-- EXPECTATION: not misspelled country
SELECT DISTINCT
	cntry 
FROM bronze.erp_loc_a101

/* ===============================
-- For TABLE erp_px_cat_g1v2:
=================================*/

SELECT 
	id,
	cat,
	subcat
	maintenance
FROM bronze.erp_px_cat_g1v2

-- the cat_id from the silver.crm_prd_info matches the bronze.erp_px_cat_g1v2 id

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
-- expected results: ('YES','No')
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2



/*
=====================================================
			DATA cleansing SCRIPT:
=====================================================
*/

/* 
===============================
-- For TABLE crm_cust_info:
=================================
*/

-- after cleaning on top of it we can insert CLEANED data into silver 
INSERT INTO silver.crm_cust_info (
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_marital_status,
  cst_gndr,
  cst_create_date )

SELECT 
	cst_id,
	cst_key,
	-- remove unwanted spaces of firstname and lastname
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,

	--full name for marital status
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END AS cst_marital_status,

	-- full name of gndr
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
		WHEN UPPER((cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr,
	
	-- no need to change create date because already a date (we defined it)
	cst_create_date
	
FROM (
-- remove duplicates
SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info 
WHERE cst_id IS NOT NULL)
WHERE flag_last = 1 



/* 
===============================
-- For TABLE crm_prd_info:
=================================
*/

INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
 SELECT 
  prd_id,

  -- Derive category_id from first 5 characters of prd_key (used for joining with crm_sales_details)
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,

  -- Extract the suffix of prd_key (used for joining with sales_details)
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,

  prd_nm,

  -- Replace NULL cost with 0
  COALESCE(prd_cost, 0) AS prd_cost,

  -- Standardize product line names
  CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'T' THEN 'Touring'
    ELSE 'n/a'
  END AS prd_line,

  	CAST(prd_start_dt AS DATE),

  -- For temporal consistency: next start date - 1 day
  CAST(LEAD(prd_start_dt) OVER (
    PARTITION BY prd_key
    ORDER BY prd_start_dt
  ) - INTERVAL '1 day' AS DATE) AS prd_end_dt

FROM bronze.crm_prd_info;


/* ===============================
-- For TABLE crm_sales_details:
=================================*/

INSERT INTO silver.crm_sales_details(
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price
)
SELECT
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  CASE 
    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
    ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
  END AS sls_order_dt,
  TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD') AS sls_ship_dt ,
  TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD') AS sls_due_dt,
  CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
  	THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
	 END AS sls_sales,
	 sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <=0
		THEN  sls_sales / NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details;


/* ===============================
-- For TABLE erp_cust_az12:
=================================*/

INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
)
SELECT 	
	CASE WHEN cid LIKE 'NAS%' THEN
		SUBSTRING(cid,4,LENGTH(cid))
		ELSE cid
	END AS cid,
	--correcting unconsistent date
	CASE WHEN bdate> NOW() THEN NULL
	 	ELSE bdate
	END AS bdate,
	-- correct unconsistent gender (gen)
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;

/* ===============================
-- For TABLE erp_loc_a101:
=================================*/

INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
)
SELECT 	
	-- remove the '-' from cid to match bronze.crm_cust_info.cst_key
	REPLACE(cid, '-', '') AS cid,
	-- standardize country
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101

/* ===============================
-- For TABLE erp_px_cat_g1v2:
=================================*/

INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance)
SELECT
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2


  
 


