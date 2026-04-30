-- ============================================================================
-- Snowflake Setup Script for eCommerce Analytics Pipeline
-- ============================================================================
-- Purpose: Create RAW schema tables and load data from stage
-- Run this in Snowflake Web UI Worksheet
-- ============================================================================

-- Set context
USE DATABASE ECOMMERCE_DB;
USE WAREHOUSE DEV_WH;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: Create Internal Stage
-- ============================================================================
-- Stage is like a temporary storage area in Snowflake for files

CREATE OR REPLACE STAGE ecommerce_stage
    FILE_FORMAT = (
        TYPE = 'CSV' 
        FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
        SKIP_HEADER = 1
    )
    COMMENT = 'Stage for eCommerce CSV files';

-- Verify stage created
SHOW STAGES;

-- ============================================================================
-- STEP 2: Create RAW Tables
-- ============================================================================

-- Customers table
CREATE OR REPLACE TABLE raw_customers (
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    country VARCHAR(50),
    signup_date DATE
)
COMMENT = 'Raw customer data from source system';

-- Products table
CREATE OR REPLACE TABLE raw_products (
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2),
    cost DECIMAL(10,2)
)
COMMENT = 'Raw product catalog';

-- Orders table
CREATE OR REPLACE TABLE raw_orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    order_status VARCHAR(20)
)
COMMENT = 'Raw order transactions';

-- Order items table
CREATE OR REPLACE TABLE raw_order_items (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_price DECIMAL(10,2)
)
COMMENT = 'Raw order line items';

-- Verify tables created
SHOW TABLES IN SCHEMA RAW;

-- ============================================================================
-- STEP 3: Load Data from Stage
-- ============================================================================
-- NOTE: Before running this, upload CSV files to the stage via:
--       - Snowflake Web UI (Data > Databases > ECOMMERCE_DB > RAW > Stages)
--       - OR SnowSQL: PUT file://data/*.csv @ecommerce_stage
-- ============================================================================

-- Load customers
COPY INTO raw_customers 
FROM @ecommerce_stage/raw_customers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Load products
COPY INTO raw_products 
FROM @ecommerce_stage/raw_products.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Load orders
COPY INTO raw_orders 
FROM @ecommerce_stage/raw_orders.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Load order items
COPY INTO raw_order_items 
FROM @ecommerce_stage/raw_order_items.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- STEP 4: Verify Data Loaded Successfully
-- ============================================================================

-- Check row counts
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM raw_customers
UNION ALL
SELECT 'products', COUNT(*) FROM raw_products
UNION ALL
SELECT 'orders', COUNT(*) FROM raw_orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM raw_order_items;

-- Expected results:
-- customers: 1000
-- products: 200
-- orders: 5000
-- order_items: 15000+

-- Sample data from each table
SELECT * FROM raw_customers LIMIT 5;
SELECT * FROM raw_products LIMIT 5;
SELECT * FROM raw_orders LIMIT 5;
SELECT * FROM raw_order_items LIMIT 5;

-- ============================================================================
-- Setup Complete!
-- ============================================================================
-- Next step: Run dbt models to transform this raw data
-- Command: dbt run --select staging
-- ============================================================================