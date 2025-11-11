-- Connect to airflow database
\connect airflow_db;

-- Create schema for airflow metadata (not going to deep dive into this portion in this case study), however, it is possible to set up some 
-- monitorying using these metadata tables as sources.
CREATE SCHEMA IF NOT EXISTS airflow AUTHORIZATION airflow_admin;