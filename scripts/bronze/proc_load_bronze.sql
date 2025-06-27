/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the COPY command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();

WARNING:
	You should create a /tmp directory from your terminal where you copy all the data to allows postgres
		to access it for the "COPY" in the stored procedure.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    RAISE NOTICE '=== Starting Bronze Layer Load ===';

    -- ----------------------------
    -- Load CRM Tables
    -- ----------------------------

    -- Truncate and load crm_cust_info
    TRUNCATE TABLE bronze.crm_cust_info;
    start_time := clock_timestamp();
    COPY bronze.crm_cust_info 
    FROM '/tmp/cust_info.csv' 
    DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_cust_info loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- Truncate and load crm_prd_info
    TRUNCATE TABLE bronze.crm_prd_info;
    start_time := clock_timestamp();
    COPY bronze.crm_prd_info 
    FROM '/tmp/prd_info.csv' 
    DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_prd_info loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- Truncate and load crm_sales_details
    TRUNCATE TABLE bronze.crm_sales_details;
    start_time := clock_timestamp();
    COPY bronze.crm_sales_details 
    FROM '/tmp/sales_details.csv' 
    DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_sales_details loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- ----------------------------
    -- Load ERP Tables
    -- ----------------------------

    -- Truncate and load erp_loc_a101
    TRUNCATE TABLE bronze.erp_loc_a101;
    start_time := clock_timestamp();
    COPY bronze.erp_loc_a101 
    FROM '/tmp/LOC_A101.csv' 
    DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_loc_a101 loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- Truncate and load erp_cust_az12
    TRUNCATE TABLE bronze.erp_cust_az12;
    start_time := clock_timestamp();
    COPY bronze.erp_cust_az12 
    FROM '/tmp/CUST_AZ12.csv' 
    DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_cust_az12 loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- Truncate and load erp_px_cat_g1v2
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    start_time := clock_timestamp();
    COPY bronze.erp_px_cat_g1v2 
    FROM '/tmp/PX_CAT_G1V2.csv' 
    DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_px_cat_g1v2 loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    RAISE NOTICE '✅ Bronze Layer Load Completed Successfully.';
END;
$$;

-- executes the stored proc
CALL bronze.load_bronze();


