-- Connect to data_layer_db
\connect data_layer_db;

-- Create schema for your raw data
CREATE SCHEMA IF NOT EXISTS pipeline AUTHORIZATION dl_admin;

SET SEARCH_PATH TO pipeline;

-- Create the base table
CREATE TABLE IF NOT EXISTS tb_raw_customer_transactions (
    transaction_id TEXT,
    customer_id TEXT,
    transaction_date TEXT,
    product_id TEXT,
    product_name TEXT,
    quantity TEXT,
    price TEXT,
    tax TEXT,
    load_datetime TIMESTAMP DEFAULT NOW());

-- Grant privilegies for this single user, in production we would have a read/write role and each user would be assigned a role.
GRANT ALL PRIVILEGES ON SCHEMA pipeline TO dl_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pipeline TO dl_admin;