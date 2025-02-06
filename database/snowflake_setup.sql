-- Create database and schema
CREATE DATABASE IF NOT EXISTS REAL_ESTATE_ANALYTICS;
USE DATABASE REAL_ESTATE_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS RAW;

-- Create external stage for Azure
CREATE OR REPLACE STAGE AZURE_STAGE
  URL = 'azure://dwhomesdatalake.blob.core.windows.net/raw-data'
  CREDENTIALS = (AZURE_SAS_TOKEN = '');

-- Create tables for HUD data
CREATE OR REPLACE TABLE RAW.FAIR_MARKET_RENTS (
    state_code VARCHAR,
    year NUMBER,
    data_content VARIANT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.INCOME_LIMITS (
    state_code VARCHAR,
    year NUMBER,
    data_content VARIANT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create tables for CRM and ERP data
CREATE OR REPLACE TABLE RAW.CRM_DATA (
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip_code VARCHAR,
    lead_source VARCHAR,
    lead_status VARCHAR,
    created_date DATE
);

CREATE OR REPLACE TABLE RAW.ERP_DATA (
    project_id VARCHAR,
    home_model VARCHAR,
    base_price NUMBER,
    square_feet NUMBER,
    lot_number VARCHAR,
    subdivision VARCHAR,
    construction_start_date DATE,
    estimated_completion_date DATE,
    status VARCHAR,
    budget NUMBER,
    actual_costs NUMBER
);
