import sys
from us_visa.exception import UsVisaException
from us_visa.logger import logging

from us_visa.components.data_ingestion import DataIngestion
from us_visa.components.data_validation import DataValidationPipeline
from us_visa.components.data_transformation import DataTransformation
from us_visa.components.model_trainer import ModelTrainer
from us_visa.components.model_evaluation import ModelEvaluation
from us_visa.components.model_pusher import ModelPusher

from us_visa.entity.config_entity import (
    DataIngestionConfig, 
    DataValidationConfig,
    DataTransformationConfig,
    ModelTrainerConfig,
    MlflowConfig,
    ModelEvaluationConfig,
    ModelPusherConfig
)

class TrainingPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        self.data_validation_config = DataValidationConfig()
        self.data_transformation_config = DataTransformationConfig()
        self.model_trainer_config = ModelTrainerConfig()
        self.mlflow_config = MlflowConfig()
        self.model_evaluation_config = ModelEvaluationConfig()
        self.model_pusher_config = ModelPusherConfig()

    def run_pipeline(self) -> None:
        try:
            logging.info("Starting training pipeline")
            
            # data_ingestion
            ingestion = DataIngestion(self.data_ingestion_config)
            ingestion_artifact = ingestion.initiate_data_ingestion()

            logging.info("Data ingestion completed")

            # data validation
            validation = DataValidationPipeline(config=self.data_validation_config)

            validation_artifact = validation.validate(
                train_path=ingestion_artifact.train_file_path,
                test_path=ingestion_artifact.test_file_path
            )

            logging.info(f"Validation status: {validation_artifact.validation_status}")

            # stop if validation fails
            if not validation_artifact.validation_status:
                logging.error("Data Validation failed. Stopping pipeline")
                logging.error(validation_artifact.message)
                return
            
            logging.info("Data Validation passed")

            # data transformation
            transformation = DataTransformation(
                ingestion_artifact=ingestion_artifact,
                validation_artifact=validation_artifact,
                config=self.data_transformation_config
            )

            transformation_artifact = transformation.initiate_data_transformation()

            logging.info("Data transformation completed")
            
            # model training
            trainer = ModelTrainer(
                data_transformation_artifact=transformation_artifact,
                config=self.model_trainer_config,
                mlflow_config=self.mlflow_config
            )

            model_trainer_artifact = trainer.initiate_model_trainer()

            logging.info("Model Training Completed")
            logging.info(f"Model path: {model_trainer_artifact.metric_artifact}")
            logging.info(f"Metrics: {model_trainer_artifact.metric_artifact}")

            # model evaluation
            evaluator = ModelEvaluation(
                config=self.model_evaluation_config,
                transformation_artifact=transformation_artifact,
                trainer_artifact=model_trainer_artifact
            )

            model_evaluation_artifact = evaluator.initiate_model_evaluation()
            logging.info(f"Model accepted: {model_evaluation_artifact.is_model_accepted}")

            # model pusher
            if not model_evaluation_artifact.is_model_accepted:
                logging.info("New model rejected. Skipping deployment.")
                return
            
            pusher = ModelPusher(
                config=self.model_pusher_config,
                evaluation_artifact=model_evaluation_artifact
            )

            model_pusher_artifact = pusher.initiate_model_pusher()

            logging.info(f"Model pusher to s3: {model_evaluation_artifact.s3_model_path}")

        except Exception as e:
            raise UsVisaException(e, sys)