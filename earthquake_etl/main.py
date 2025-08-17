import requests
import polars as pl
import json
from utils.load_config import load_config
import logging
from utils.mongodb_insgester import MongoDBIngester

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

def task3(data: dict):
    df = pl.DataFrame(data)

    print(df)


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

    task3(data["features"])


if __name__ == "__main__":
    main()
