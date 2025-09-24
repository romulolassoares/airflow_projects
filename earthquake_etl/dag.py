from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import polars as pl
import json
import logging
import pyodbc
import pandas as pd
from earthquake_etl.utils.load_config import load_config
from earthquake_etl.utils.mongodb_ingester import MongoDBIngester


# Define the DAG
dag = DAG(
    'earthquake_etl_pipeline',
    description='ETL pipeline for earthquake data from USGS API',
    catchup=False,
)

def extract_data_from_api(**context):
    logger = logging.getLogger("earthquake_etl")

    feed_type = "all_day"
    base_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary"
    url = f"{base_url}/{feed_type}.geojson"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        logger.info(f"Successfully extracted {len(data.get('features', []))} earthquake records")

        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from API: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON response: {e}")
        raise

def insert_raw_data_to_mongo(**context):

    logger = logging.getLogger("earthquake_etl")
    config = load_config("./earthquake_etl/config.yaml")

    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract_data_task')

    if not data or 'features' not in data:
        raise ValueError("No data received from extraction task")

    mongo_ingester = MongoDBIngester(
        mongo_uri=config["mongo_db"]["uri"],
        database_name=config["mongo_db"]["database"],
        collection_name=config["mongo_db"]["collection_name"],
        logger=logger
    )

    try:
        mongo_ingester.connect()
        mongo_ingester.insert_many(data["features"])

        duplicate_result = mongo_ingester.remove_duplicates(
            unique_fields="id",
            dry_run=False
        )

        logger.info(f"Inserted data to MongoDB. Duplicate removal result: {duplicate_result}")

        return len(data["features"])

    except Exception as e:
        logger.error(f"Failed to insert data to MongoDB: {e}")
        raise
    finally:
        mongo_ingester.disconnect()

def process_data(**context):
    logger = logging.getLogger("earthquake_etl")

    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract_data_task')

    if not data or 'features' not in data:
        raise ValueError("No data received from extraction task")

    try:
        standardized_data = []

        for row in data["features"]:
            value = {
                "id": row["id"],
                "longitude": row["geometry"]["coordinates"][0],
                "latitude": row["geometry"]["coordinates"][1],  # Fixed typo
                "depth": row["geometry"]["coordinates"][2],
                **row["properties"]
            }
            standardized_data.append(value)

        df = pl.DataFrame(standardized_data)
        df = df.drop(["url", "detail"])

        logger.info(f"Processed {len(df)} earthquake records")

        return df.to_pandas().to_dict('records')

    except Exception as e:
        logger.error(f"Failed to process data: {e}")
        raise

def load_to_sqlserver(**context):
    logger = logging.getLogger("earthquake_etl")
    config = load_config("./earthquake_etl/config.yaml")

    ti = context['ti']
    data_records = ti.xcom_pull(task_ids='process_data_task')

    if not data_records:
        raise ValueError("No processed data received")

    df_pandas = pd.DataFrame(data_records)

    try:
        conn = pyodbc.connect(
            f"DRIVER={config['sqlserver']['DRIVER']};"
            f"SERVER={config['sqlserver']['SERVER']};"
            f"DATABASE={config['sqlserver']['DATABASE']};"
            f"UID={config['sqlserver']['UID']};"
            f"PWD={config['sqlserver']['PWD']}"
        )
        cursor = conn.cursor()

        columns = df_pandas.columns.tolist()

        cursor.execute("DROP TABLE IF EXISTS geo_json_data")
        cols_sql = ', '.join([f"[{col}] NVARCHAR(MAX)" for col in columns])
        cursor.execute(f"CREATE TABLE geo_json_data ({cols_sql})")

        placeholders = ', '.join(['?' for _ in columns])
        insert_sql = f"INSERT INTO geo_json_data VALUES ({placeholders})"

        for _, row in df_pandas.iterrows():
            values = [str(val) if not pd.isna(val) else None for val in row]
            cursor.execute(insert_sql, values)

        conn.commit()
        logger.info(f"Successfully inserted {len(df_pandas)} records to SQL Server")

        return len(df_pandas)

    except Exception as e:
        logger.error(f"Failed to load data to SQL Server: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def data_quality_check(**context):
    logger = logging.getLogger("earthquake_etl")

    ti = context['ti']
    extracted_count = len(ti.xcom_pull(task_ids='extract_data_task').get('features', []))
    mongo_count = ti.xcom_pull(task_ids='insert_raw_data_task')
    processed_count = len(ti.xcom_pull(task_ids='process_data_task'))
    sqlserver_count = ti.xcom_pull(task_ids='load_to_sqlserver_task')

    logger.info(f"Data Quality Check:")
    logger.info(f"  - Extracted: {extracted_count} records")
    logger.info(f"  - MongoDB: {mongo_count} records")
    logger.info(f"  - Processed: {processed_count} records")
    logger.info(f"  - SQL Server: {sqlserver_count} records")

    if processed_count != sqlserver_count:
        raise ValueError(f"Data count mismatch: Processed {processed_count} but loaded {sqlserver_count}")

    return "Data quality check passed"

extract_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data_from_api,
    dag=dag,
)

mongo_insert_task = PythonOperator(
    task_id='insert_raw_data_task',
    python_callable=insert_raw_data_to_mongo,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    dag=dag,
)

sqlserver_load_task = PythonOperator(
    task_id='load_to_sqlserver_task',
    python_callable=load_to_sqlserver,
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id='data_quality_check_task',
    python_callable=data_quality_check,
    dag=dag,
)

extract_task >> [mongo_insert_task, process_task]
process_task >> sqlserver_load_task >> quality_check_task