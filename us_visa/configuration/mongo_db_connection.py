import sys

from us_visa.exception import UsVisaException
from us_visa.logger import logging

import os
from us_visa.constants import DATABASE_NAME, MONGODB_URL_KEY
import pymongo
import certifi


ca = certifi.where()

class MongoDBClient:
    """
    Provides a reusable MongoDB connection.

    Uses a class-level client to ensure only one connection is created
    and shared across all instances.
    """
    client = None

    def __init__(self, database_name=DATABASE_NAME) -> None:
        try:
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv(MONGODB_URL_KEY)

                if not mongo_db_url:
                    raise ValueError(
                        f"Missing environment variable: {MONGODB_URL_KEY}"
                    )
                
                logging.info("Initializing MongoDB Connection")

                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
            
            self.client = MongoDBClient.client
            print(f"Clienti: {self.client}")
            self.database = self.client[database_name]
            print(f"Databasi: {self.database}")
            self.database_name = database_name
            
            logging.info("Mongo Connection Successful")
        
        except Exception as e:
            logging.error("Failed to initialize MongoDB connection", exc_info=True)
            raise UsVisaException(e, sys)