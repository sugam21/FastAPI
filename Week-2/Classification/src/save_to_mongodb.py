import datetime
import json

from loguru import logger
from pymongo import MongoClient


class Mongo:
    def __init__(self, client: str = "localhost", port: int = 27017):
        """Initialize a mongodb client and select the database."""
        try:
            self.client = MongoClient(host=client, port=port)
            self.db = self.client["bfhl"]
            logger.info(
                f"Connected to mongodb successfully. Server info: {self.client.server_info()}"
            )
        except Exception as e:
            logger.error(str(e))
            self.client = None
            self.db = None

    def insert_record(self, collection_name: str, record: dict) -> bool:
        """Insert a record to the collection"""
        if self.db is None:
            logger.error("Database connection is not initialized.")
            return False
        try:
            collection = self.db[collection_name]
            result = collection.insert_one(record)
            logger.info(f"Record inserted successfully with ID: {result.inserted_id}")

        except Exception as e:
            logger.error(str(e))
            return False
        else:
            self.client.close()
            logger.info("MongoDB connection closed.")
            return result.inserted_id


# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":

    # Get the database
    db = Mongo()
    record: dict = {
        "timestamp": datetime.datetime.now(),
        "input": {"col1": 1, "col2": 2, "col3": 3},
        "output": "Diabetic",
        "model_used": "LGBM",
        "response_time": 0.001,
    }
    record_json_searlized = json.dumps(record, indent=4, sort_keys=True, default=str)
    result = db.insert_record(collection_name="Diabetes", record=record)
