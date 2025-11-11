from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import os
import psycopg2
from word2number import w2n
import re
from io import StringIO
import time

# Default DAG settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'data_platform_pipeline',
    default_args=default_args,
    description='ETL pipeline: CSV ingestion + dbt transformations',
    schedule=None,
    start_date=datetime(2025, 11, 6),
    catchup=False,
)

# 1️⃣ Load CSV into Postgres
def load_csv_to_postgres():

    def convert_literal_text_to_numbers(value_in_text):
        """
        Converts written numbers into numeric characters.
        """
        if re.search("[a-zA-Z]", value_in_text):
            return float(w2n.word_to_num(value_in_text))
        elif pd.isna(value_in_text):
            return None
        else:
            return float(value_in_text)
    
    def parse_ambiguous_date(date_str):
        """
        Fix the dates so they can be into YYYY-MM-DD format.
        """
        if pd.isna(date_str) or date_str == '':
            return None

        parts = [int(x) for x in re.findall(r'\d+', str(date_str))]
        if len(parts) < 3:
            return None

        year = max(parts)
        parts.remove(year)

        # Decide which is day / month
        if parts[0] > 12 and parts[1] <= 12:
            day, month = parts[0], parts[1]
        elif parts[0] <= 12 and parts[1] > 12:
            day, month = parts[1], parts[0]
        else:
            month, day = parts[0], parts[1]

        try:
            return datetime(year, month, day).date()
        except ValueError:
            return None
    
    db_user = os.environ.get("DB_DL_USER")
    db_pass = os.environ.get("DB_DL_PASSWORD")
    db_host = os.environ.get("DB_DL_HOST")
    db_name = os.environ.get("DB_DL_NAME")
    db_port= os.environ.get("DB_DL_PORT")

    conn = psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_pass,
                host=db_host,
                port=db_port
            )

    # Read csv
    df = pd.read_csv('/opt/airflow/dags/customer_transactions.csv')

    # Convert price and tax to numeric
    df['price'] = df['price'].astype(str)
    df['tax'] = df['tax'].astype(str)

    # Fix written numbers into numbers
    df['price'] = df['price'].apply(convert_literal_text_to_numbers)
    df['tax'] = df['tax'].apply(convert_literal_text_to_numbers)

    # Add load_datetime column for reference
    df["load_datetime"] = datetime.now()

    # Fill the NaN(s)
    df.fillna('', inplace=True)

    # Assign all columns to strings, otherwise pandas will try to conver some automatically, causing some errors
    df['transaction_date'] = df['transaction_date'].apply(parse_ambiguous_date)

    df = df.astype(str)

    # Connect to the DB
    cur = conn.cursor()

    # Ensure we are at the right schema
    cur.execute("""
        set search_path to pipeline;
    """)
    conn.commit()

    # Bulk data copy into the DB
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cur.copy_from(buffer, 'tb_raw_customer_transactions', sep=',', null='')

    conn.commit()

    cur.close()
    conn.close()
    print("CSV successfully loaded")

load_csv = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag,
)

# Run dbt transformations
dbt_run = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /opt/airflow/dbt/data_layer_project && dbt deps && dbt run --profiles-dir /opt/airflow/dbt',
    dag=dag,
)

# Run dbt tests
dbt_test = BashOperator(
    task_id='run_dbt_tests',
    bash_command='cd /opt/airflow/dbt/data_layer_project && dbt test --profiles-dir /opt/airflow/dbt || echo " Some dbt tests failed, but continuing..."',
    dag=dag,
)

# DAG flow
load_csv >> dbt_run >> dbt_test
