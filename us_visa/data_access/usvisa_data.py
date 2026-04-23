from us_visa.configuration.mongo_db_connection import MongoDBClient
from us_visa.constants import DATABASE_NAME
from us_visa.exception import UsVisaException
import pandas as pd
import sys
from typing import Optional
import numpy as np


class USVisaData:
    """
    This class helps to export entire MongoDb records as dataframe
    """

    def __init__(self):
        """
        """
        try:
            self.mongo_client = MongoDBClient(database_name=DATABASE_NAME)
        except Exception as e:
            raise UsVisaException(e, sys)
        
    
    def export_collection_as_dataframe(self, collection_name: str, database_name:Optional[str]=None)->pd.DataFrame:
        try:
            """
            export entire collection as dataframe
            return pd.DataFrame of collection
            """

            if database_name:
                db = self.mongo_client.client[database_name]
            else:
                db = self.mongo_client.database

            print(f"database Name: {db.name}")
            print(f"Collections: {db.list_collection_names()}")

            collection = db[collection_name]

            print(f"Using collection: {collection_name}")
            print(f"Document Count: {collection.count_documents({})}")

            # fetch + convert
            records = list(collection.find().batch_size(1000))
            print(f"Fetched records: {len(records)}")

            if len(records) == 0:
                raise ValueError(f"No data found in the collection {collection_name}")

            df = pd.DataFrame(records)
            # drop mongo uuid
            if '_id' in df.columns:
                df = df.drop(columns=['_id'], axis=1)

            df.replace({"na": np.nan}, inplace=True)
            return df
        
        except Exception as e:
            raise UsVisaException(e, sys)