import requests
import polars as pl
import json
from utils.load_config import load_config
import logging
from utils.mongodb_insgester import MongoDBIngester
import pyodbc
import pandas as pd
import configparser

def task1_extract_data_from_api(config: dict, logger: logging.Logger):
    feed_type = "all_day"

    base_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary"
    url = f"{base_url}/{feed_type}.geojson"

    response = requests.get(url)
    return response

def task2_insert_raw_data_to_mongo(ingester: MongoDBIngester, data):
    ingester.connect()
    ingester.insert_many(data)
    res = ingester.remove_duplicates(unique_fields="id", dry_run=False)
    ingester.disconnect()

def task3_process_data(data: dict):
    standardized_data = []
    for row in data:
        value = {
            "id": row["id"],
            "longitude": row["geometry"]["coordinates"][0],
            "lagitude": row["geometry"]["coordinates"][1],
            "depth": row["geometry"]["coordinates"][2],
            **row["properties"]
        }
        standardized_data.append(value)

    df = pl.DataFrame(standardized_data)

    df = df.drop(["url","detail"])

    return df

def task4(df: pl.DataFrame, config: dict):
    conn = pyodbc.connect(
        f"DRIVER={config['sqlserver']['DRIVER']};"
        f"SERVER={config['sqlserver']['SERVER']};"
        f"DATABASE={config['sqlserver']['DATABASE']};"
        f"UID={config['sqlserver']['UID']};"
        f"PWD={config['sqlserver']['PWD']}"
    )
    cursor = conn.cursor()

    df_pandas = df.to_pandas()
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
    cursor.close()
    conn.close()
    print("Dados inseridos!")

def main():
    logger = logging.getLogger("Application")

    config = load_config("./earthquake_etl/config.yaml")

    mongo_ingester = MongoDBIngester(
        mongo_uri=config["mongo_db"]["uri"],
        database_name=config["mongo_db"]["database"],
        collection_name=config["mongo_db"]["collection_name"],
        logger=logger
    )

    data = task1_extract_data_from_api(config, logger)
    data = data.json()

    task2_insert_raw_data_to_mongo(mongo_ingester, data["features"])

    df = task3_process_data(data["features"])
    task4(df,config)

if __name__ == "__main__":
    main()
