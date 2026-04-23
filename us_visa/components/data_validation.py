import os
import sys
import json
from typing import Dict, Any
from pandas import DataFrame

import numpy as np
import pandas as pd
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset

from us_visa.exception import UsVisaException
from us_visa.logger import logging
from us_visa.utils.main_utils import read_yaml, write_yaml_file
from us_visa.entity.config_entity import DataValidationConfig
from us_visa.entity.artifact_entity import DataValidationArtifact, DataIngestionArtifact
from us_visa.constants import SCHEMA_FILE_PATH

def _make_json_serializable(obj):
    if isinstance(obj, dict):
        return {k: _make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_make_json_serializable(i) for i in obj]
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating, )):
        return float(obj)
    elif isinstance(obj, (np.bool_, )):
        return bool(obj)
    
    return obj

TYPE_VALIDATORS = {
    "int": pd.api.types.is_integer_dtype,
    "float": pd.api.types.is_float_dtype,
    "category": pd.api.types.is_object_dtype
}

class DataValidationSchemaValidator:
    def __init__(self):
        try:
            self.schema = read_yaml(SCHEMA_FILE_PATH)
            self.expected_cols = set(self.schema['columns'].keys())
        except Exception as e:
            raise UsVisaException(e, sys)

    def validate(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate schema: missing columns, extra columns, data types
        """
        actual_cols = set(df.columns)

        missing_cols = list(self.expected_cols - actual_cols)
        extra_cols = list(actual_cols - self.expected_cols)

        type_mismatches = {}

        # check for datatypes mismatches
        for col, expected_type in self.schema["columns"].items():
            if col in df.columns:
                # get validator function
                validator = TYPE_VALIDATORS.get(expected_type)

                # fall back if type not in registry
                if validator is None:
                    actual_dtype = str(df[col].dtype)
                    type_mismatches[col] = actual_dtype
                    continue

                # run validation
                if not validator(df[col]):
                    type_mismatches[col] = str(df[col].dtype)


        return {
            "missing_columns": missing_cols,
            "extra_columns": extra_cols,
            "type_mismatches": type_mismatches
        }


class DataQualityValidator:
    def __init__(self, null_threshold: float=0.2):
        self.null_threshold = null_threshold

    def validate_nulls(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate if any col exceed the null threshold
        """
        null_report = {}
        for col in df.columns:
            null_ratio = df[col].isnull().mean()
            if null_ratio > self.null_threshold:
                null_report[col] = float(null_ratio)
        
        return null_report
    
    def validate_duplicates(self, df: DataFrame) -> int:
        """
        Validate the no of duplicated rows
        """
        return df.duplicated().sum()
    

class DataDriftValidator:
    def __init__(self, drift_report_file_path: str):
        self.drift_report_file_path = drift_report_file_path

    def detect_drift(self, reference_df: DataFrame, current_df: DataFrame) -> Dict[str, Any]:
        """
        Detect drift between the reference & Current DataFrame
        """
        try:
            report = Report(metrics=[DataDriftPreset()])
            report.run(reference_data=reference_df, current_data=current_df)

            result = report.as_dict()

            # save drift report
            write_yaml_file(filepath=self.drift_report_file_path, content=result)

            drift_status = result["metrics"][0]["result"].get("dataset_drift", False)

            return {"drift_detected": drift_status, "full_report": result}
        except Exception as e:
            raise UsVisaException(e, sys)
        
class DataValidationPipeline:
    def __init__(self, config: DataValidationConfig):
        self.config = config
        self.schema_validator = DataValidationSchemaValidator()
        self.quality_validator = DataQualityValidator(null_threshold=self.config.data_validation_null_threshold)
        self.drift_validator = DataDriftValidator(self.config.drift_report_file_path)

    def validate(self, train_path: str, test_path: str) -> DataValidationArtifact:
        """
        The main pipeline for validating data
        """
        try:
            logging.info("Starting data validation pipeline")

            # load dataset
            train_df = pd.read_csv(train_path)
            test_df = pd.read_csv(test_path)

            # initiate the result dictionary
            validation_report = {
                "train": {"schema": {}, "quality": {}},
                "test": {"schema": {}, "quality": {}}
            }

            # -------- Schema validation -------
            train_schema = self.schema_validator.validate(train_df)
            test_schema = self.schema_validator.validate(test_df)
            validation_report["train"]['schema'] = train_schema
            validation_report["test"]['schema'] = test_schema

            # ---------- Data Quality checks -------
            train_quality = {
                "nulls": self.quality_validator.validate_nulls(train_df),
                "duplicates": self.quality_validator.validate_duplicates(train_df)
            }
            test_quality = {
                "nulls": self.quality_validator.validate_nulls(test_df),
                "duplicates": self.quality_validator.validate_duplicates(test_df)
            }
            validation_report["train"]["quality"] = train_quality
            validation_report["test"]["quality"] = test_quality

            # ----------- Drift Detection ---------
            drift_result = self.drift_validator.detect_drift(train_df, test_df)
            validation_report['drift'] = drift_result

            # ------- Final Decisions --------
            has_schema_errors = any([
                len(train_schema["missing_columns"]) > 0, 
                len(train_schema["extra_columns"]) > 0, 
                len(train_schema["type_mismatches"]) > 0, 
                len(test_schema["missing_columns"]) > 0,
                len(test_schema["extra_columns"]) > 0,
                len(test_schema["type_mismatches"]) > 0
            ])

            has_quality_issues = (
                len(train_quality["nulls"]) > 0 or 
                len(test_quality["nulls"]) > 0 or 
                train_quality["duplicates"] > 0 or 
                test_quality["duplicates"] > 0
            )

            drift_detected = drift_result["drift_detected"]

            # overall validation status
            validation_status = not (has_schema_errors or has_quality_issues or drift_detected)

            result_message = {
                "validation_passes": validation_status,
                "schema_errors": {"train": train_schema, "test": test_schema},
                "quality_issues": {"train": train_quality, "test": test_quality},
                "drift": drift_result
            }

            logging.info(f"Validation status: {validation_status}")

            safe_result_message = _make_json_serializable(result_message)
            return DataValidationArtifact(
                validation_status=validation_status,
                message=json.dumps(safe_result_message, indent=2),
                drift_report_file_path=self.config.drift_report_file_path
            )
        except Exception as e:
            logging.error(f"Data validation pipeline failed {str(e)}")
            raise UsVisaException(e, sys)