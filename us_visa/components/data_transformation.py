import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Tuple

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, OrdinalEncoder, PowerTransformer

from imblearn.combine import SMOTEENN

from us_visa.constants import TARGET_COLUMN, SCHEMA_FILE_PATH, CURRENT_YEAR
from us_visa.entity.config_entity import DataTransformationConfig
from us_visa.entity.artifact_entity import DataTransformationArtifact, DataIngestionArtifact, DataValidationArtifact
from us_visa.entity.estimator import TargetValueMapping
from us_visa.utils.main_utils import (
    read_yaml,
    write_yaml_file,
    save_object,
    save_numpy_array_data
)

from us_visa.exception import UsVisaException
from us_visa.logger import logging

# --------- Feature Engineering ---------
class FeatureEngineering(BaseEstimator, TransformerMixin):
    def __init__(self, current_year: int):
        self.current_year = current_year

    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        X = X.copy()
        if "yr_of_estab" in X.columns:
            X["company_age"] = self.current_year - X["yr_of_estab"]
        
        return X
    

# --------- Data Transformation ----------
class DataTransformation:
    def __init__(
        self,
        ingestion_artifact: DataIngestionArtifact,
        config: DataTransformationConfig,
        validation_artifact: DataValidationArtifact
    ):
        try:
            self.ingestion_artifact = ingestion_artifact
            self.config = config
            self.validation_artifact = validation_artifact
            self.schema = read_yaml(SCHEMA_FILE_PATH)
        except Exception as e:
            raise UsVisaException(e, sys)
        
    # read_data
    @staticmethod
    def read_data(path: str) -> pd.DataFrame:
        return pd.read_csv(path)
    
    # Build pipeline
    def _build_pipeline(self) -> Pipeline:
        try:
            oh_cols = self.schema.oh