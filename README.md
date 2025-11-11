# Data Platform Pipeline

This project implements a **data pipeline** using **Airflow 3**, **DBT**, and **PostgreSQL**, orchestrated via **Docker Compose**.  
The pipeline extracts, loads, and transforms customer transaction data into a PostgreSQL database and builds analytics models with DBT.

---

## Table of Contents

- [Project Structure](#project-structure)  
- [Requirements](#requirements)  
- [Setup](#setup)  
- [Environment Variables](#environment-variables)  
- [Running the Pipeline](#running-the-pipeline)  
- [Airflow Login](#airflow-ui)  
---

## Project Structure
├── airflow/
│ ├── dags/ # Airflow DAG files
│ ├── requirements.txt # Python dependencies for Airflow
├── dbt/
│ ├── data_layer_project/ # DBT project
│ └── profiles.yml # DBT profiles configuration
├── postgres/
│ ├── init/ # SQL scripts to initialize DB
├── Dockerfile.airflow # Dockerfile for Airflow
├── Dockerfile.dbt # Dockerfile for DBT
├── docker-compose.yml
└── README.md
└── .env # Environment variables for Airflow


## Requirements

- Docker >= 24.x  
- Docker Compose >= 2.x  

---

## Setup

1. Clone the repository:

```bash
git clone <repo-url>
cd <repo-folder>

## Environment Variables
At the root of the project, create a .env file with the following variables:
```
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = postgresql+psycopg2://airflow_admin:airflow_admin@postgres:5432/airflow_db
POSTGRES_DB = db
POSTGRES_USER = db
POSTGRES_PASSWORD = db_password

DB_DL_USER=dl_admin
DB_DL_PASSWORD=dl_admin
DB_DL_HOST=postgres
DB_DL_NAME=data_layer_db
DB_DL_PORT=5432
```

## Running the Pipeline
At the root of the project, please run
```
docker-compose up --build
```

## Airflow Login

1. Getting the credentials
Once services are up and running, since Airflow service is a standalone version, scroll up until you see this image to grab the password of the **admin** user:
<img width="915" height="362" alt="image" src="https://github.com/user-attachments/assets/a957a49e-13b4-472b-b661-f162bcd087b0" />

2. Running the pipeline
Go to data_platform_pipeline and trigger the dag:
<img width="947" height="228" alt="image" src="https://github.com/user-attachments/assets/43f4b7b0-9a6b-41ee-809a-9f3944cc8fac" />



