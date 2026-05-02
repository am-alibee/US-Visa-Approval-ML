import sys
from pandas import DataFrame

from us_visa.cloud_storage.aws_storage import SimpleStorageService

from us_visa.logger import logging
from us_visa.exception import UsVisaException
from us_visa.entity.estimator import USVisaModel


class USVisaEstimator:
    def __init__(self, bucket_name: str, model_path: str):
        self.bucket = bucket_name
        self.key =  model_path
        self.storage = SimpleStorageService()
        self._model: USVisaModel = None

    def is_model_present(self) -> bool:
        return self.storage.file_exists(self.bucket, self.key)
    
    def load_model(self) -> USVisaModel:
        if self._model is None:
            self._model = self.storage.load_model(self.bucket, self.key)
        return self._model
    
    def save_model(self, local_path: str, remove: bool = False):
        self.storage.upload_file(local_path, self.bucket, self.key)

    def predict(self, df: DataFrame):
        try:
            model = self.load_model()
            return model.predict(df)
        except Exception as e:
            raise UsVisaException(e, sys)