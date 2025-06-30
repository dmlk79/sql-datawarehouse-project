/* ===============================================================================
Stored Procedures: Load Bronze and Silver Layers
===============================================================================
Script Purpose:
    This script sequentially executes two stored procedures to build the data pipeline:
    
    1. bronze.load_bronze():
        - Loads data into the 'bronze' schema from external CSV files.
        - Truncates existing bronze tables before loading.
        - Uses the COPY command to import data from files.

    2. silver.load_silver():
        - Transforms, cleans, and standardizes data from the bronze layer.
        - Loads the processed data into the 'silver' schema.

Parameters:
    None.
    Both stored procedures do not accept any parameters or return values.

Usage Example:
    CALL bronze.load_bronze();
    CALL silver.load_silver();

WARNING:
    Make sure the CSV files are placed inside a `/tmp` directory accessible to PostgreSQL. 
    This is required for the COPY command used in `bronze.load_bronze()`.
===============================================================================
*/

-- executes the stored proc to load source systems data into bronze layer
CALL bronze.load_bronze();

/* executes the following stored procedure to transform, clean, standadize
	data for bronze to SILVER and load it into silver layer:
*/

CALL silver.load_silver();

