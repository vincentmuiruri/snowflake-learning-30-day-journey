/*==================================================================================
Project: BANK FRAUD DETECTION PIPELINE
-----------------------------------------------------------------------------------
Author: Vincent Mbira
Purpose: Demonstrate how to create a data pipeline in Snowflake dynamic tables.
Key Concepts Covered:
- Raw data ingestion layer
- Clean data transformation layer
- Analytics aggregation layer
- Automatic incremental refresh
- Real-time fraud monitoring
==================================================================================*/

/*=================================================================================
STEP 1: DATABASE SETUP & SCHEMA SETUP
-----------------------------------------------------------------------------------
Creating the database and schemas for the data pipeline
==================================================================================*/
-- -- deleting the database
-- DROP DATABASE fraud_demo;

-- setting up the database
CREATE OR REPLACE DATABASE fraud_demo;

-- defining the schemas
CREATE OR REPLACE SCHEMA fraud_demo.raw;
CREATE OR REPLACE SCHEMA fraud_demo.analytics;

-- state the database to be used
USE DATABASE fraud_demo;

/*===================================================================================
STEP 2: RAW DATA TABLE CREATION
------------------------------------------------------------------------------------
Captures transaction data for fraud monitoring and analysis
=====================================================================================*/
CREATE OR REPLACE TABLE fraud_demo.raw.transactions_raw (
    transaction_id    NUMBER,
    transaction_ts    TIMESTAMP_NTZ,
    account_id        NUMBER,
    customer_id       NUMBER,
    amount            NUMBER(12,2),
    transaction_type  STRING,
    merchant_name     STRING,
    merchant_category STRING,
    location_city     STRING,
    location_country  STRING,
    device_id         STRING,
    ip_address        STRING,
    fraud_flag        BOOLEAN,
    risk_score        NUMBER(3,2),
    status            STRING
);

SELECT * FROM FRAUD_DEMO.raw.transactions_raw;

/*===================================================================================
STEP 3: DEMO DATA LOAD
-------------------------------------------------------------------------------------
* Generate demo data with 10 transactions for checking.
* Demo data has transactions that are legitimate, suspicious, and fraudulent
* Risk scores are rated ranging from 0,0 (low risk) to 1.00 (high_risk)
=====================================================================================*/
-- Clear existing data
TRUNCATE TABLE IF EXISTS fraud_demo.raw.transactions_raw;

-- Inserting data into the table
INSERT INTO fraud_demo.raw.transactions_raw VALUES
-- Legitimate transactions
(1001, '2024-01-01 09:15:00', 50001, 101, 45.99, 'PURCHASE', 'Starbucks', 'FOOD_BEVERAGE', 'New York', 'USA', 'DEV001', '192.168.1.100', FALSE, 0.12, 'APPROVED'),
(1002, '2024-01-01 10:30:00', 50002, 102, 125.50, 'PURCHASE', 'Amazon', 'RETAIL', 'Seattle', 'USA', 'DEV002', '192.168.1.101', FALSE, 0.08, 'APPROVED'),
(1003, '2024-01-01 11:45:00', 50001, 101, 2500.00, 'ATM_WITHDRAWAL', 'Chase Bank ATM', 'BANKING', 'New York', 'USA', 'DEV001', '192.168.1.100', FALSE, 0.25, 'APPROVED'),

-- Suspicious transactions (high risk but not confirmed fraud)
(1004, '2024-01-01 12:00:00', 50003, 103, 8500.00, 'WIRE_TRANSFER', 'Unknown Recipient', 'TRANSFER', 'Lagos', 'Nigeria', 'DEV003', '41.203.45.22', FALSE, 0.78, 'PENDING_REVIEW'),
(1005, '2024-01-01 12:15:00', 50002, 102, 3200.00, 'PURCHASE', 'Luxury Watches Ltd', 'JEWELRY', 'Hong Kong', 'China', 'DEV999', '103.45.67.89', FALSE, 0.82, 'PENDING_REVIEW'),

-- Confirmed fraud transactions
(1006, '2024-01-01 13:00:00', 50004, 104, 15000.00, 'WIRE_TRANSFER', 'Offshore Account', 'TRANSFER', 'Unknown', 'Cayman Islands', 'DEV888', '185.220.101.45', TRUE, 0.95, 'DECLINED'),
(1007, '2024-01-01 13:30:00', 50001, 101, 5000.00, 'PURCHASE', 'Electronics Depot', 'ELECTRONICS', 'Moscow', 'Russia', 'DEV777', '95.142.33.78', TRUE, 0.91, 'DECLINED'),
(1008, '2024-01-01 14:00:00', 50005, 105, 9999.99, 'ATM_WITHDRAWAL', 'Street ATM', 'BANKING', 'Unknown', 'Romania', 'DEV666', '89.47.201.33', TRUE, 0.98, 'DECLINED');

-- confirm data insertion
SELECT * FROM FRAUD_DEMO.RAW.TRANSACTIONS_RAW;

/*=======================================================================================
STE[P 4: CLEAN LAYER - DYNAMIC TABLE
-----------------------------------------------------------------------------------------
I used dynamic tables for the following reasons:
* Transaction data comes into the db automatically and continuously
* To get only cleaned and approved transactions for clean analytics.
* To ennsure incremental refresh without streams/ tasks. 

THE TARGET LAG = '1 minute'
- Indicates acceptable freshness delay 
- Snowflake decides when and how to refresh
- The warehouse that we use is the compute_wh
========================================================================================*/
CREATE OR REPLACE DYNAMIC TABLE fraud_demo.analytics.transactions_clean
TARGET_LAG = '1 minute'
WAREHOUSE = compute_wh
AS 
SELECT 
    transaction_id,
    transaction_ts,
    account_id,
    customer_id,
    amount,
    transaction_type,
    merchant_name,
    merchant_category,
    location_city,
    location_country,
    device_id,
    ip_address,
    risk_score
FROM fraud_demo.raw.transactions_raw
WHERE status = 'APPROVED' 
AND fraud_flag = 'FALSE';

-- check the inserted data
SELECT * FROM fraud_demo.analytics.transactions_clean;

/*====================================================================
STEP 5: REFRESH VALIDATION
----------------------------------------------------------------------
Insert new raw data and observe automatic refresh.
No manual job execution is required.
=====================================================================*/

INSERT INTO fraud_demo.raw.transactions_raw VALUES
(1009, CURRENT_TIMESTAMP, 50006, 106, 50.75, 'PURCHASE', 'Target Store', 'RETAIL', 'Chicago', 'USA', 'DEV004', '192.168.1.105', FALSE, 0.15, 'APPROVED'),
(1010, CURRENT_TIMESTAMP, 50006, 106, 1250.35, 'GASOLINE', 'North East Gas', 'RETAIL', 'Austin', 'USA', 'DEV004', '197.128.1.102', FALSE, 0.15, 'APPROVED'),
(1011, CURRENT_TIMESTAMP, 50006, 106, 25.15, 'AIRTIME', 'Starlink', 'INTERNET', 'Seattle', 'USA', 'DEV004', '192.148.1.101', FALSE, 0.15, 'APPROVED');

/*
After -1 minute run the query below
SELECT * FROM fraud_demo.analytics.transactions_clean;
The new records will be seen
*/
-- viewing the added records
SELECT * FROM fraud_demo.analytics.transactions_clean;

/*=========================================================================================
STEP 6A: ANALYTICS LAYER - AGGREGATED DYNAMIC TABLE
-------------------------------------------------------------------------------------------
* This Dynamic Table depends on another dynamic table.
* Snowflake automatically:
    - Tracks dependencies
    - Refreshes only impacted data
    - Preserves incremental behavior

* Aggregates daily transaction metrics for fraud monitoring
=========================================================================================*/
CREATE OR REPLACE DYNAMIC TABLE fraud_demo.analytics.daily_transaction_summary
TARGET_LAG = '5 minutes'
WAREHOUSE = compute_wh
AS
SELECT 
    DATE(transaction_ts) AS transaction_date,
    merchant_category,
    location_country,
    COUNT(transaction_id) AS total_transactions,
    SUM(amount) AS total_amount,
    AVG(amount) AS  avg_amount,
    AVG(risk_score) AS avg_risk_score,
    MAX(risk_score) AS max_risk_score,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM fraud_demo.analytics.transactions_clean
GROUP BY DATE(transaction_ts), merchant_category, location_country;

-- confirm the inserted data
SELECT * FROM fraud_demo.analytics.daily_transaction_summary;

/*=========================================================================
SECTION 6B: FRAUD ANALYTICS LAYER - RISK MONITORING
---------------------------------------------------------------------------
- Separate dynamic table for fraud specific metrics including both approved
and flagged transactions for risk analysis.
=========================================================================*/
CREATE OR REPLACE DYNAMIC TABLE fraud_demo.analytics.fraud_risk_summary
TARGET_LAG = '2 minutes'
WAREHOUSE = compute_wh
AS
SELECT
    DATE(transaction_ts) AS transaction_date,
    location_country,
    COUNT(transaction_id) AS total_transactions,
    SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END) AS fraud_count,
    SUM(CASE WHEN status = 'DECLINED' THEN 1 ELSE 0 END) AS declined_count,
    SUM(CASE WHEN status = 'PENDING_REVIEW' THEN 1 ELSE 0 END) AS pending_review_count,
    AVG(risk_score) AS avg_risk_score,
    SUM(CASE WHEN fraud_flag = TRUE THEN amount ELSE 0 END) AS fraud_amount_blocked,
    ROUND(SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END)*100.0 / 
    COUNT(transaction_id),2) AS fraud_rate_percentage
FROM fraud_demo.raw.transactions_raw
GROUP BY DATE(transaction_ts), location_country;

-- confirm the inserted data
SELECT * FROM fraud_demo.analytics.fraud_risk_summary;

/*==========================================================================
STEP 7: ANALYTICS QUERIES
---------------------------------------------------------------------------
These tables are BI/ dashboard ready for fraud monitoring.
==========================================================================*/

-- Query 1: Daily transaction summary
SELECT * 
FROM fraud_demo.analytics.daily_transaction_summary
ORDER BY transaction_date DESC, 
         total_amount DESC;

-- Query 2: Fraud risk summary by country
SELECT * 
FROM fraud_demo.analytics.fraud_risk_summary
WHERE fraud_count > 0
ORDER BY fraud_rate_percentage DESC;


-- Query 3: High-risk countries analysis
SELECT 
    location_country,
    SUM(total_transactions) AS total_txns,
    SUM(fraud_count) AS total_fraud,
    AVG(fraud_rate_percentage) AS avg_fraud_rate,
    SUM(fraud_amount_blocked) AS total_blocked_amount
FROM fraud_demo.analytics.fraud_risk_summary
GROUP BY location_country
HAVING SUM(fraud_count) > 0
ORDER BY avg_fraud_rate DESC;

/*=======================================================
STEP 8: OBSERVABILITY & METADATA
---------------------------------------------------------
This step is for monitoring refresh behavior of tables and debugging
=========================================================*/
SHOW DYNAMIC TABLES;

DESCRIBE DYNAMIC TABLE fraud_demo.analytics.transactions_clean;
DESCRIBE DYNAMIC TABLE fraud_demo.analytics.daily_transaction_summary;
DESCRIBE DYNAMIC TABLE fraud_demo.analytics.fraud_risk_summary;

-- Check refresh history
USE DATABASE fraud_demo;
USE SCHEMA analytics;

SELECT 
    name,
    target_lag_sec,
    state,
    refresh_end_time,
    refresh_action,
    refresh_trigger
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'fraud_demo.analytics.transactions_clean'
));

SELECT *
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
LIMIT 1;

/*==============================================================
 KEY TAKEAWAY
----------------------------------------------------------------
 Dynamic Tables enable declarative fraud detection pipelines:
 - No Streams
 - No Tasks
 - No manual scheduling
 - Incremental by default
 - Real-time fraud monitoring without complex orchestration
 - Automatic dependency management across layers
 
 Benefits for Fraud Detection:
 - Clean layer filters approved transactions for analytics
 - Fraud summary tracks risk metrics across all statuses
 - Sub-minute latency for critical fraud alerts
 - Simplified pipeline maintenance
================================================================*/