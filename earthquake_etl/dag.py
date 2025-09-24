from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import requests
import polars as pl
import pandas as pd
import pyodbc
from utils.mongodb_insgester import MongoDBIngester
import configparser

# --- CONFIG ---
CONFIG_PATH = "/path/to/config.ini"

def load_config_ini(file_path: str):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

config = load_config_ini(CONFIG_PATH)

logger = logging.getLogger("earthquake_etl")

# --- TASK FUNCTIONS ---
def extract_data_from_api(**kwargs):
    feed_type = "all_day"
    base_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary"
    url = f"{base_url}/{feed_type}.geojson"
    response = requests.get(url)
    data = response.json()
    # Push data to XCom for next tasks
    kwargs['ti'].xcom_push(key='raw_data', value=data)
    logger.info(f"Extracted {len(data['features'])} earthquake records")
    return data

def insert_raw_data_to_mongo(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='raw_data', task_ids='extract_data')
    mongo_ingester = MongoDBIngester(
        mongo_uri=config['mongo']['uri'],
        database_name=config['mongo']['database'],
        collection_name=config['mongo']['collection_name'],
        logger=logger
    )
    mongo_ingester.connect()
    mongo_ingester.insert_many(data['features'])
    mongo_ingester.remove_duplicates(unique_fields='id', dry_run=False)
    mongo_ingester.disconnect()
    logger.info("Inserted raw data into MongoDB")

def process_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='raw_data', task_ids='extract_data')

    standardized_data = []
    for row in data['features']:
        value = {
            "id": row["id"],
            "longitude": row["geometry"]["coordinates"][0],
            "latitude": row["geometry"]["coordinates"][1],
            "depth": row["geometry"]["coordinates"][2],
            **row["properties"]
        }
        standardized_data.append(value)

    df = pl.DataFrame(standardized_data)
    df = df.drop(["url","detail"])

    # Push processed data to XCom
    kwargs['ti'].xcom_push(key='processed_df', value=df.to_pandas().to_dict('records'))
    logger.info(f"Processed {len(df)} records")

def insert_data_to_sql(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(key='processed_df', task_ids='process_data')
    df = pd.DataFrame(records)

    conn = pyodbc.connect(
        f"DRIVER={config['sqlserver']['DRIVER']};"
        f"SERVER={config['sqlserver']['SERVER']};"
        f"DATABASE={config['sqlserver']['DATABASE']};"
        f"UID={config['sqlserver']['UID']};"
        f"PWD={config['sqlserver']['PWD']}"
    )
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS geo_json_data")
    cols_sql = ', '.join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
    cursor.execute(f"CREATE TABLE geo_json_data ({cols_sql})")

    placeholders = ', '.join(['?' for _ in df.columns])
    insert_sql = f"INSERT INTO geo_json_data VALUES ({placeholders})"

    for _, row in df.iterrows():
        values = [str(val) if not pd.isna(val) else None for val in row]
        cursor.execute(insert_sql, values)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Inserted processed data into SQL Server")

# --- DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'earthquake_etl',
    default_args=default_args,
    description='ETL DAG to process earthquake data',
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_from_api,
        provide_context=True
    )

    insert_mongo = PythonOperator(
        task_id='insert_mongo',
        python_callable=insert_raw_data_to_mongo,
        provide_context=True
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True
    )

    insert_sql_task = PythonOperator(
        task_id='insert_sql',
        python_callable=insert_data_to_sql,
        provide_context=True
    )

    # --- TASK DEPENDENCIES ---
    extract_data >> insert_mongo >> process_data_task >> insert_sql_task
    