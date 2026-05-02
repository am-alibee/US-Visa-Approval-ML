import sys
from dataclasses import dataclass
from typing import Optional

import numpy as np
from sklearn.metrics import f1_score

from us_visa.exception import UsVisaException
from us_visa.logger import logging
from us_visa.utils.main_utils import load_numpy_array_data, load_object

from us_visa.entity.s3_estimator import USVisaEstimator
from us_visa.entity.config_entity import ModelEvaluationConfig

from us_visa.entity.artifact_entity import (
    ModelTrainerArtifact,
    DataTransformationArtifact,
    ModelEvaluationArtifact
)


@dataclass
class EvaluateModelResult:
    trained_f1: float
    prod_f1: float
    is_accepted: bool
    improvement: float


class ModelEvaluation:
    def __init__(
        self,
        config: ModelEvaluationConfig,
        transformation_artifact: DataTransformationArtifact,
        trainer_artifact: ModelTrainerArtifact
    ):
        self.config = config
        self.transformation_artifact = transformation_artifact
        self.trainer_artifact = trainer_artifact
        
    def _load_test_data(self):
        test_arr = load_numpy_array_data(
            self.transformation_artifact.transformed_test_file_path
        )

        x_test = test_arr[:, :-1]
        y_test = test_arr[:, -1]

        return x_test, y_test
    
    def _get_production_model(self) -> Optional[USVisaEstimator]:
        estimator = USVisaEstimator(
            bucket_name=self.config.bucket_name,
            model_path=self.config.s3_model_key_path
        )

        if estimator.is_model_present():
            return estimator

        logging.info("No production model found in S3")
        return None
    
    def evaluate(self) -> EvaluateModelResult:
        try:
            x_test, y_test = self._load_test_data()

            # load newly trained model
            trained_model = load_object(
                self.trainer_artifact.trained_model_file_path
            )

            y_pred = trained_model.predict(x_test)
            trained_f1 = f1_score(y_test, y_pred)

            logging.info(f"Trained Model F1: {trained_f1}")

            # production model
            prod_estimator = self._get_production_model()
            prod_f1 = 0.0

            if prod_estimator:
                prod_model = prod_estimator.load_model()
                y_prod = prod_model.predict(x_test)
                prod_f1 = f1_score(y_test, y_prod)

            improvement = trained_f1 - prod_f1
            is_accepted = improvement > self.config.changed_threshold_score

            return EvaluateModelResult(
                trained_f1=trained_f1,
                prod_f1=prod_f1,
                is_accepted=is_accepted,
                improvement=improvement
            )
        
        except Exception as e:
            raise UsVisaException(e, sys)

    def initiate_model_evaluation(self) -> ModelEvaluationArtifact:
        try:
            result = self.evaluate()

            logging.info(f"Evaluation results: {result}")

            return ModelEvaluationArtifact(
                is_model_accepted=result.is_accepted,
                s3_model_path=self.config.s3_model_key_path,
                trained_model_path=self.trainer_artifact.trained_model_file_path,
                changed_accuracy=result.improvement
            )
        except Exception as e:
            raise UsVisaException(e, sys)