import sys
from us_visa.exception import UsVisaException
from us_visa.logger import logging

from us_visa.components.data_ingestion import DataIngestion
from us_visa.components.data_validation import DataValidationPipeline
from us_visa.components.data_transformation import DataTransformation
from us_visa.components.model_trainer import ModelTrainer

from us_visa.entity.config_entity import (
    DataIngestionConfig, 
    DataValidationConfig,
    DataTransformationConfig,
    ModelTrainerConfig,
    MlflowConfig
)
from us_visa.entity.artifact_entity import (
    DataIngestionArtifact, 
    DataValidationArtifact,
    DataTransformationArtifact,
    ModelTrainerArtifact
)

class TrainingPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        self.data_validation_config = DataValidationConfig()
        self.data_transformation_config = DataTransformationConfig()
        self.model_trainer_config = ModelTrainerConfig()
        self.mlflow_config = MlflowConfig()

    # def start_data_ingestion(self) -> DataIngestionArtifact:

    #     try:
    #         logging.info("Entered the start_data_ingestion method of TrainingPipeline")
    #         logging.info("Getting the data from MongoDB")
    #         data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config)
    #         data_ingestion_artifacts = data_ingestion.initiate_data_ingestion()
    #         logging.info("Got the training & test sets from MongoDB")
    #         logging.info("Exited the start_data_ingestion method the TrainingPipeline class")

    #         return data_ingestion_artifacts
    #     except Exception as e:
    #         raise UsVisaException(e, sys)
        
    # def start_data_validation(self, data_ingestion_artifact: DataIngestionArtifact) -> DataValidationArtifact:
    #     """
    #     This method of the train pipeline is responsible for starting the datavalidation component
    #     """
    #     logging.info("Entered the start_data_validation component of training pipeline")
    #     try:
    #         data_validation = DataValidationPipeline()
        
        
    # def run_pipeline(self) -> None:

    #     try:
    #         data_ingestion_artifact = self.start_data_ingestion()

    #     except Exception as e:
    #         raise UsVisaException(e, sys)

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


        except Exception as e:
            raise UsVisaException(e, sys)