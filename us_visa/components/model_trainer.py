import sys
import importlib
import numpy as np
import optuna
import mlflow
import mlflow.sklearn

from sklearn.metrics import f1_score, precision_score, recall_score, accuracy_score
from sklearn.pipeline import Pipeline

from us_visa.exception import UsVisaException
from us_visa.logger import import logging
from us_visa.utils.main_utils import (
    load_numpy_array_data,
    load_object,
    save_object,
    read_yaml
)

from us_visa.entity.config_entity import ModelTrainerConfig
from us_visa.entity.artifact_entity import (
    DataTransformationArtifact,
    ModelTrainerArtifact,
    ClassificationMetricArtifact
)

def get_class(module_name: str, class_name: str):
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


class ModelTrainer:
    def __init__(self, data_transformation_artifact: DataTransformationArtifact, config: ModelTrainerConfig):
        self.data_transformation_artifact = data_transformation_artifact
        self.config = config
        self.schema = read_yaml(config.model_config_file_path)

        