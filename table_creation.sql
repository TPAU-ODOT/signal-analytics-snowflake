-- ================================================================================================
-- Signal Analytics Snowflake Database Setup Script
-- ================================================================================================
-- This script creates all the required database objects for the signal analytics pipeline.
-- Run this manually in Snowflake before executing the Python setup scripts.
-- ================================================================================================

-- Check current database and schema context
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

-- ================================================================================================
-- FILE FORMATS
-- ================================================================================================

-- Check existing RITIS CSV format (if exists)
DESC FILE FORMAT TPAU_DB.TPAU_RITIS_SCHEMA.RITISCSVFORMAT;

-- Create Parquet file format for data processing
CREATE OR REPLACE FILE FORMAT TPAU_DB.TPAU_RITIS_SCHEMA.RITISPARQUETFORMAT
TYPE = 'PARQUET'
COMPRESSION = 'SNAPPY'
BINARY_AS_TEXT = FALSE;

-- ================================================================================================
-- STAGES
-- ================================================================================================

-- Create stage for storing stored procedure code and dependencies
CREATE OR REPLACE STAGE CODE_STAGE;

-- ================================================================================================
-- SECRETS
-- ================================================================================================

-- Create secret for RITIS API key (replace 'x' with actual API key)
CREATE OR REPLACE SECRET SHAWNS_RITIS_KEY
  TYPE = GENERIC_STRING
  SECRET_STRING = 'x';

-- ================================================================================================
-- DIMENSION TABLES
-- ================================================================================================

-- Table for XD connectivity relationships
CREATE TABLE TPAU_DB.TPAU_RITIS_SCHEMA.DIM_CONNECTIVITY (
    XD INT,
    next_XD INT
);

-- Table for signal/XD mapping with geographic and metadata information
CREATE TABLE TPAU_DB.TPAU_RITIS_SCHEMA.DIM_SIGNALS_XD (
    ID VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    valid_geometry BOOLEAN,
    XD INT,
    bearing VARCHAR,
    county VARCHAR,
    roadName VARCHAR,
    miles DOUBLE,
    approach BOOLEAN,
    extended BOOLEAN
);

-- ================================================================================================
-- FACT/DATA TABLES
-- ================================================================================================

-- Main travel time data table (raw INRIX XD data from RITIS)
CREATE TABLE XD_TRAVEL_TIME (
    XD_ID INTEGER,
    MEASUREMENT_TSTAMP TIMESTAMP,
    TRAVEL_TIME_SECONDS FLOAT
);

-- Analytics results table with predictions and anomaly flags
CREATE OR REPLACE TABLE TRAVEL_TIME_ANALYTICS (
    XD INT,
    TIMESTAMP TIMESTAMP,
    TRAVEL_TIME_SECONDS NUMBER,
    prediction NUMBER,
    anomaly BOOLEAN,
    originated_anomaly BOOLEAN
);

-- Changepoint detection results
CREATE TABLE CHANGEPOINTS (
    XD INTEGER,
    TIMESTAMP TIMESTAMP,
    SCORE FLOAT,
    AVG_BEFORE FLOAT,
    AVG_AFTER FLOAT,
    AVG_DIFF FLOAT,
    PCT_CHANGE FLOAT
);

-- Free flow travel time reference data
CREATE TABLE FREEFLOW (
    XD INTEGER,
    TRAVEL_TIME_SECONDS FLOAT
);

-- ================================================================================================
-- PIPELINE MANAGEMENT TABLES
-- ================================================================================================

-- Pipeline execution tracking and orchestration table
CREATE OR REPLACE TABLE run_stats (
  Date DATE,
  StartTime TIMESTAMP,
  Duration INT,
  Type VARCHAR,
  Completed BOOLEAN DEFAULT FALSE,
  Attempts INT DEFAULT 0,
  Error_Msg VARCHAR
);

-- ================================================================================================
-- SAMPLE DATA AND VERIFICATION QUERIES
-- ================================================================================================

-- Insert initial pipeline tracking records (example dates)
INSERT INTO run_stats (Date, Type)
VALUES 
  ('2025-09-03', 'download'),
  ('2025-09-03', 'analytics');

-- ================================================================================================
-- DATA VERIFICATION QUERIES (Optional - for testing)
-- ================================================================================================

-- Check connectivity data
-- SELECT * FROM DIM_CONNECTIVITY LIMIT 5;

-- Check travel time data
-- SELECT * FROM XD_TRAVEL_TIME LIMIT 5;
-- SELECT MAX(measurement_tstamp) FROM XD_TRAVEL_TIME;
-- SELECT DISTINCT(measurement_tstamp::date) as date FROM XD_TRAVEL_TIME ORDER BY date;

-- Check analytics results
-- SELECT * FROM TRAVEL_TIME_ANALYTICS LIMIT 5;

-- Check changepoints
-- SELECT * FROM CHANGEPOINTS LIMIT 5;

-- Check freeflow data  
-- SELECT * FROM FREEFLOW LIMIT 5;

-- Check pipeline status
-- SELECT * FROM run_stats;

-- ================================================================================================
-- CLEANUP COMMANDS (Use when needed to reset tables)
-- ================================================================================================

-- Truncate analytics tables (uncomment when needed)
-- TRUNCATE TABLE TRAVEL_TIME_ANALYTICS;
-- TRUNCATE TABLE CHANGEPOINTS;

-- ================================================================================================
-- NOTES
-- ================================================================================================
-- 1. Replace 'x' in SHAWNS_RITIS_KEY with your actual RITIS API key
-- 2. Ensure you have the proper database (TPAU_DB) and schema (TPAU_RITIS_SCHEMA) context
-- 3. This script assumes you have appropriate permissions to create these objects
-- 4. The pipeline expects these tables to be populated with dimension data before running
-- 5. The stored procedures created by the Python setup scripts will reference these tables
-- ================================================================================================
