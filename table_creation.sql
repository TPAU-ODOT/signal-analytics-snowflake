
-- Raw data table from RITIS API
CREATE TABLE XD_TRAVEL_TIME (
    XD_ID INTEGER,
    MEASUREMENT_TSTAMP TIMESTAMP,
    TRAVEL_TIME_SECONDS FLOAT
);

-- Table that maps XD segments to their next segment for point source anomaly detection
CREATE TABLE TPAU_DB.TPAU_RITIS_SCHEMA.DIM_CONNECTIVITY (
    XD INT,
    next_XD INT
);

-- Table that maps XD segments to Traffic Signals (ID)
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

-- Table to store travel time analytics results
CREATE OR REPLACE TABLE TRAVEL_TIME_ANALYTICS (
    XD INT,
    TIMESTAMP TIMESTAMP,
    TRAVEL_TIME_SECONDS NUMBER,
    prediction NUMBER,
    anomaly BOOLEAN,
    originated_anomaly BOOLEAN
);

-- Table to store change point detection results
CREATE TABLE CHANGEPOINTS (
    XD INTEGER,
    TIMESTAMP TIMESTAMP,
    SCORE FLOAT,
    AVG_BEFORE FLOAT,
    AVG_AFTER FLOAT,
    AVG_DIFF FLOAT,
    PCT_CHANGE FLOAT
);

-- Table to store free flow travel times for each XD segment
CREATE TABLE FREEFLOW (
    XD INTEGER,
    TRAVEL_TIME_SECONDS FLOAT
);

-- Create geometry table for XD segments
CREATE TABLE XD_GEOM (
  XD INTEGER,
  GEOM GEOGRAPHY
);

--Create a run stats table for tracking pipeline runs
CREATE
OR REPLACE TABLE run_stats (
  Date DATE,
  StartTime TIMESTAMP,
  Duration INT,
  Type VARCHAR,
  Completed BOOLEAN DEFAULT FALSE,
  Attempts INT DEFAULT 0,
  Error_Msg VARCHAR
);
-- Seed with dates you want to begin at
INSERT INTO
  run_stats (Date, Type)
VALUES
  ('2025-09-03', 'download'),
  ('2025-09-03', 'analytics');

-- This is where all Python code/packages will be staged for stored procedures
CREATE OR REPLACE STAGE CODE_STAGE;

-- Secret to store RITIS API key securely
CREATE OR REPLACE SECRET SHAWNS_RITIS_KEY -- Use your own name
  TYPE = GENERIC_STRING
  SECRET_STRING = 'x'; -- Use your own RITIS API key