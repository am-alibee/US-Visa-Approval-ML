import boto3
import os
import sys
from botocore.config import Config

from us_visa.constants import (
    AWS_ACCESS_KEY_ENV_KEY,
    AWS_SECRET_ACCESS_KEY_ENV_KEY,
    REGION_NAME
)

from us_visa.exception import UsVisaException
from us_visa.logger import logging

class S3Client:
    def __init__(self, region_name: str = REGION_NAME):
        try:
            self.region_name = region_name

            # retry strategy 
            self.boto_config = Config(
                retries={
                    "max_attempts": 5,
                    "mode": "standard"
                }
            )

            access_key = os.getenv(AWS_ACCESS_KEY_ENV_KEY)
            secret_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY)

            if access_key and secret_key:
                logging.info("Using AWS credentials from enviroment variables")

                self.s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    region_name=self.region_name,
                    config=self.boto_config
                )
            
            else:
                # fall back -> iam role
                logging.info("Using AWS IAM role / default credential chain")

                self.s3_client = boto3.client(
                    "s3",
                    region_name=region_name,
                    config=self.boto_config
                )

        except Exception as e:
            raise UsVisaException(e, sys)