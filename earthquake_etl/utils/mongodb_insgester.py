import pymongo
from pymongo import MongoClient
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class MongoDBIngester:
    def __init__(self, mongo_uri: str, database_name: str, collection_name: str, logger: logging.Logger):
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def connect(self):
        try:
            self.client = MongoClient(self.mongo_uri)
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            self.logger.info("Successfully connected to MongoDB!")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to MongoDB: {e}")
            return False

    def disconnect(self):
        if self.client:
            self.client.close()
            self.logger.info("Mongo connection closed")

    def insert_one(self, document: Dict[str, Any]) -> Optional[str]:
        if self.collection is None:
            self.logger.error("Not connected to MongoDB. Call connect() first.")
            return None

        try:
            result = self.collection.insert_one(document)
            self.logger.info(f"Document inserted with ID: {result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            self.logger.error(f"Error inserting document: {e}")
            return None

    def insert_many(self, documents: List[Dict[str, Any]]) -> Optional[List[str]]:
        if  self.collection is None:
            self.logger.error("Not connected to MongoDB. Call connect() first.")
            return None

        try:
            result = self.collection.insert_many(documents)
            self.logger.info(f"{len(result.inserted_ids)} documents inserted successfully!")
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            self.logger.error(f"Error inserting documents: {e}")
            return None

    def remove_duplicates(self, unique_fields: Union[str, List[str]],
                          keep_strategy: str = "first",
                          dry_run: bool = False) -> Dict[str, Any]:
        if self.collection is None:
            self.logger.error("Not connected to MongoDB. Call connect() first.")
            return {"error": "Not connected to database"}

        # Convert single field to list
        if isinstance(unique_fields, str):
            unique_fields = [unique_fields]

        try:
            # Build aggregation pipeline to find duplicates
            group_fields = {field: f"${field}" for field in unique_fields}

            pipeline = [
                {
                    "$group": {
                        "_id": group_fields,
                        "docs": {"$push": {"id": "$_id", "doc": "$$ROOT"}},  # Correção: $$ROOT em vez de $ROOT
                        "count": {"$sum": 1}
                    }
                },
                {
                    "$match": {"count": {"$gt": 1}}
                }
            ]

            duplicates = list(self.collection.aggregate(pipeline))

            if not duplicates:
                self.logger.info("No duplicates found")
                return {
                    "duplicates_found": 0,
                    "documents_removed": 0,
                    "unique_field_combinations": 0
                }

            total_duplicates = sum(group["count"] for group in duplicates)
            duplicate_combinations = len(duplicates)

            self.logger.info(f"Found {total_duplicates} duplicate documents in {duplicate_combinations} groups")

            if dry_run:
                return {
                    "duplicates_found": total_duplicates,
                    "documents_removed": 0,
                    "unique_field_combinations": duplicate_combinations,
                    "dry_run": True,
                    "duplicate_groups": [
                        {
                            "fields": group["_id"],
                            "count": group["count"],
                            "document_ids": [doc["id"] for doc in group["docs"]]
                        } for group in duplicates
                    ]
                }

            # Remove duplicates based on strategy
            removed_count = 0

            for group in duplicates:
                docs = group["docs"]

                # Sort documents based on keep strategy
                if keep_strategy == "first":
                    # Keep first inserted (smallest ObjectId)
                    docs_to_remove = docs[1:]
                elif keep_strategy == "last":
                    # Keep last inserted (largest ObjectId)
                    docs_to_remove = docs[:-1]
                elif keep_strategy == "newest":
                    # Sort by _id descending, keep newest
                    docs.sort(key=lambda x: x["id"], reverse=True)
                    docs_to_remove = docs[1:]
                elif keep_strategy == "oldest":
                    # Sort by _id ascending, keep oldest
                    docs.sort(key=lambda x: x["id"])
                    docs_to_remove = docs[1:]
                else:
                    self.logger.error(f"Invalid keep_strategy: {keep_strategy}")
                    return {"error": f"Invalid keep_strategy: {keep_strategy}"}

                # Remove duplicate documents
                ids_to_remove = [doc["id"] for doc in docs_to_remove]
                result = self.collection.delete_many({"_id": {"$in": ids_to_remove}})
                removed_count += result.deleted_count

                self.logger.debug(f"Removed {result.deleted_count} duplicates for fields {group['_id']}")

            self.logger.info(f"Successfully removed {removed_count} duplicate documents")

            return {
                "duplicates_found": total_duplicates,
                "documents_removed": removed_count,
                "unique_field_combinations": duplicate_combinations,
                "keep_strategy": keep_strategy
            }

        except Exception as e:
            self.logger.error(f"Error removing duplicates: {e}")
            return {"error": str(e)}