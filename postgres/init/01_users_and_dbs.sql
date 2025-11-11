-- Create user and database to AIRFLOW Metadata
CREATE USER airflow_admin WITH PASSWORD 'airflow_admin';
CREATE DATABASE airflow_db OWNER airflow_admin;

-- Create user and database to DATA LAYER
CREATE USER dl_admin WITH PASSWORD 'dl_admin';
CREATE DATABASE data_layer_db OWNER dl_admin;
