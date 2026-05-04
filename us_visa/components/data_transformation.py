import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Tuple

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, PowerTransformer

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
        if "yr_of_estab" not in X.columns:
            raise ValueError("yr_of_estab column missing for feature engineering")
        
        # create new feature
        X["company_age"] = self.current_year - X["yr_of_estab"]

        # drop original column after using it
        X = X.drop(columns=['yr_of_estab'])
        
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
            
            self.final_features = self.schema["final_features"]
        except Exception as e:
            raise UsVisaException(e, sys)
        
    # read_data
    @staticmethod
    def read_data(path: str) -> pd.DataFrame:
        # return pd.read_csv(path)

        df = pd.read_csv(path)
        cols = df.columns
        logging.info(f"the DataFrame has cols: {cols}")

        return df
    
    # Build pipeline
    def _build_pipeline(self) -> Pipeline:
        try:
            oh_cols = self.schema.oh_columns
            or_cols = self.schema.or_columns
            power_cols = self.schema.power_columns

            preprocessor = ColumnTransformer(
                transformers=[
                    ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False), oh_cols),
                    ("ordinal", OrdinalEncoder(), or_cols),
                    ("power", PowerTransformer(method="yeo-johnson"), power_cols) 
                ],
                remainder="drop",
                verbose_feature_names_out=False
            )

            pipeline = Pipeline(steps=[
                ("feature_engineering", FeatureEngineering(CURRENT_YEAR)),
                ("preprocessing", preprocessor)
            ])

            return pipeline
        
        except Exception as e:
            raise UsVisaException(e, sys)
    
    # --------- save metadata ---------
    def _save_metadata(self, x_train_final, y_before, y_after):
        try:
            metadata = {
                "timestamp": str(datetime.now()),
                "feature_count": int(x_train_final.shape[1]),
                "expected_features": self.final_features,
                "train_class_distribution_before": y_before.value_counts().to_dict(),
                "train_class_distribution_after": y_after.value_counts().to_dict()
            }

            write_yaml_file(self.config.data_transformation_meta_data, metadata)
        except Exception as e:
            raise UsVisaException(e, sys)
        
    #-------- The main transformation ---------
    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            if not self.validation_artifact.validation_status:
                raise ValueError(f"Validation Failed: {self.validation_artifact.message}")
            
            logging.info("Starting data transformation")

            # load data
            train_df = self.read_data(path=self.ingestion_artifact.train_file_path)
            test_df = self.read_data(path=self.ingestion_artifact.test_file_path)

            # Drop columns
            drop_columns = self.schema.drop_columns
            train_df = train_df.drop(columns=drop_columns)
            test_df = test_df.drop(columns=drop_columns)


            # split X/y
            x_train = train_df.drop(columns=[TARGET_COLUMN])
            y_train = train_df[TARGET_COLUMN]

            x_test = test_df.drop(columns=[TARGET_COLUMN])
            y_test = test_df[TARGET_COLUMN]

            # Encode Target
            mapping = TargetValueMapping().to_numeric()
            y_train = y_train.replace(mapping)
            y_test = y_test.replace(mapping)

            # Build pipeline
            pipeline = self._build_pipeline()

            # Transform 
            x_train = pipeline.fit_transform(x_train)
            x_test = pipeline.transform(x_test)

            # force float
            x_train = np.asarray(x_train, dtype=np.float32)
            x_test = np.asarray(x_test, dtype=np.float32)

            logging.info(f"Transformed feature shape: {x_train.shape}")

            # feature count lock
            # feature_count = x_train_final.shape[1]

            # Apply SMOTE on training
            smote = SMOTEENN()
            x_train_final, y_train_final = smote.fit_resample(x_train, y_train.astype(int))
            
            x_test_final, y_test_final = x_test, y_test.astype(int)

            #  consistency check after smote
            if x_train_final.shape[1] != x_test_final.shape[1]:
                raise ValueError("Train/Test feature mismatch after smote")


            # save the metadata
            self._save_metadata(x_train_final, y_before=y_train, y_after=y_train_final)

            # Combine arrays
            train_arr = np.c_[x_train_final, y_train_final]
            test_arr = np.c_[x_test_final, y_test_final]

            # Save Artifacts
            save_object(self.config.transformed_object_file_path, pipeline)
            save_numpy_array_data(self.config.transformed_train_file_path, train_arr)
            save_numpy_array_data(self.config.transformed_test_file_path, test_arr)

            logging.info("Data Transformation Completed Successfully")

            return DataTransformationArtifact(
                transformed_object_file_path=self.config.transformed_object_file_path,
                transformed_train_file_path=self.config.transformed_train_file_path,
                transformed_test_file_path=self.config.transformed_test_file_path
            )
        
        except Exception as e:
            raise UsVisaException(e, sys)