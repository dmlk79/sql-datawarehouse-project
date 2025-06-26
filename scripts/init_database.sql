/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates three schemas: bronze,silver and gold schema.
	
WARNING:
You should first manually create DB DataWarehouse, then connect to it in pgAdmin or psql
  
*/


-- Create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
