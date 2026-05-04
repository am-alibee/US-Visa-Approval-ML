import os

from us_visa.logger import logging
from us_visa.exception import UsVisaException

from us_visa.entity.config_entity import ModelPusherConfig
from us_visa.entity.artifact_entity import (
    ModelEvaluationArtifact,
    ModelPusherArtifact
)

from us_visa.entity.s3_estimator import USVisaEstimator
# from us_visa.utils.main_utils import load_object

class ModelPusher:
    def __init__(
        self,
        config: ModelPusherConfig,
        evaluation_artifact: ModelEvaluationArtifact
    ):
        self.config = config
        self.evaluation_artifact = evaluation_artifact

    def initiate_model_pusher(self) -> ModelPusherArtifact:
        try:
            if not self.evaluation_artifact.is_model_accepted:
                logging.info(f"Model rejected. Improvement={self.evaluation_artifact.changed_accuracy}")
                return ModelPusherArtifact(
                    bucket_name=self.config.bucket_name,
                    s3_model_path=None
                )
            logging.info("Pushing model to S3...")

            estimator = USVisaEstimator(
                bucket_name=self.config.bucket_name,
                model_path=self.config.s3_model_key_path
            )

            estimator.save_model(
                local_path=self.evaluation_artifact.trained_model_path
            )
            
            logging.info("Model successfully uploaded")

            return ModelPusherArtifact(
                bucket_name=self.config.bucket_name,
                s3_model_path=self.config.s3_model_key_path
            )

        except Exception as e:
            raise UsVisaException(e, sys)