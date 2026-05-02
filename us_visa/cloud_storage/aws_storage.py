import os
import sys
import pickle
from typing import Optional

from botocore.exceptions import ClientError

from us_visa.configuration.aws_connection import S3Client
from us_visa.logger import logging
from us_visa.exception import UsVisaException

class SimpleStorageService:
    def __init__(self):
        client = S3Client()
        self.s3_client = client.s3_client

    def file_exists(self, bucket: str, key: str) -> bool:
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False
        
    def upload_file(self, local_path: str, bucket: str, key: str):
        try:
            logging.info(f"Uploading {local_path} -> s3://{bucket}/{key}")
            self.s3_client.upload_file(local_path, bucket, key)
        except Exception as e:
            raise UsVisaException(e, sys)
    
    def download_file(self, bucket: str, key: str, local_path: str):
        try:
            logging.info(f"Downloading s3://{bucket}{key} -> {local_path}")
            self.s3_client.download_file(bucket, key, local_path)
        except Exception as e:
            raise UsVisaException(e, sys)
        
    def save_model(self, model, bucket: str, key: str, tmp_path: str = "temp_model.pkl"):
        try:
            with open(tmp_path, "wb") as f:
                pickle.dump(model, f)

            self.upload_file(tmp_path, bucket, key)
            os.remove(tmp_path)
        
        except Exception as e:
            raise UsVisaException(e, sys)
        
    def load_model(self, bucket: str, key: str, tmp_path: str = "temp_model.pkl"):
        try:
            self.download_file(bucket, key, tmp_path)

            with open(tmp_path, "rb") as f:
                model = pickle.load(f)

            os.remove(tmp_path)
            return model
        
        except Exception as e:
            raise UsVisaException(e, sys)
