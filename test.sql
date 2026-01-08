/*==================================================================================
Project: CREATING SNOWFLAKE OBJECTS
-----------------------------------------------------------------------------------
Author: Vincent Mbira
Purpose: Demonstrate how to create Snowflake objects.
Key Concepts Covered:
- Creating objects (warehouse, database, schemas, and tables)
- Staging data files and copying data into target tables
- Querying loaded data
- Summary and review of the data
==================================================================================*/

/*=================================================================================
STEP 1: CREATING SNOWFLAKE OBJECTS
-----------------------------------------------------------------------------------
Creating the database and schemas
==================================================================================*/

CREATE OR REPLACE DATABASE sf_tuts;

-- selecting the warehpuse, db and schema in use
SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();

-- Creating a table
CREATE OR REPLACE TABLE emp_basic (
    first_name STRING,
    last_name STRING,
    email STRING,
    streetaddress STRING,
    city STRING,
    start_date STRING
);

--Creating virtual warehouse
CREATE OR REPLACE WAREHOUSE sf_tuts_wh WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 100
    AUTO_RESUME = TRUE -- this setting causes a warehouse to automatically start when SQL statements requiring compute resources are executed
    INITIALLY_SUSPENDED = TRUE; --initially suspended

-- showing current warehouse
SELECT CURRENT_WAREHOUSE();

/*=================================================================================
STEP 2: STAGING DATA FILES
-----------------------------------------------------------------------------------
Uploading sample data files into the db
-----------------------------------------------------------------------------------
* Go to Data → Databases → sf_tuts → public → emp_basic
* Click Load Data
* Browse and select your CSV files
* Follow the wizard to load them
==================================================================================*/

/*=================================================================================
STEP 3: QUERYING LOADED DATA
-----------------------------------------------------------------------------------
Run different queries on the emp_basic table
==================================================================================*/
-- see all the data
SELECT * FROM emp_basic;

-- combining employee names into full name
SELECT 
    CONCAT(FIRST_NAME,' ',LAST_NAME) AS full_name,
    EMAIL,
    STREETADDRESS,
    CITY,
    START_DATE
FROM emp_basic;

-- inserting additional rows into the employees table
INSERT INTO emp_basic VALUES
   ('Clementine','Adamou','cadamou@sf_tuts.com','10510 Sachs Road','Klenak','2017-9-22') ,
   ('Marlowe','De Anesy','madamouc@sf_tuts.co.uk','36768 Northfield Plaza','Fangshan','2017-1-26');

--Querying employees from UK (use email address as reference)
SELECT * FROM emp_basic WHERE email LIKE '%.uk';

--Queying the results based on start date
SELECT first_name, last_name, DATEADD('day',90,start_date) AS started_90_days FROM emp_basic WHERE start_date <= '2017-01-01';