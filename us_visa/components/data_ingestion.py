import os
import sys

from pandas import DataFrame
from sklearn.model_selection import train_test_split

from us_visa.entity.config_entity import DataIngestionConfig
from us_visa.entity.artifact_entity import DataIngestionArtifact
from us_visa.exception import UsVisaException
from us_visa.logger import logging
from us_visa.data_access.usvisa_data import USVisaData
from us_visa.utils.main_utils import create_directories


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig = DataIngestionConfig()):
        """
        :param data_ingestion_config: configuration for data ingestion
        """
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise UsVisaException(e, sys)
        
    
    def export_data_into_feature_store(self) -> DataFrame:
        """
        Method Name:    export data into feature store
        Description:    This method export data from mongodb to csv file

        Output:         data is returned as artifact of data ingestion components
        On Fail         Write an exception log & raise an exception
        """

        try:
            logging.info("Exporting data from mongodb")
            usvisa_data = USVisaData()
            dataframe = usvisa_data.export_collection_as_dataframe(
                collection_name=self.data_ingestion_config.collection_name
            )

            logging.info(f"Shape of dataframe: {dataframe.shape}")
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            create_directories([dir_path])
            logging.info(f"Saving Exported Files into feature store file path {feature_store_file_path}")
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            
            return dataframe
        except Exception as e:
            raise UsVisaException(e, sys)
        
    def split_data_as_train_test(self, dataframe: DataFrame) -> None:
        """
        Method Name :   split_data_as_train_test
        Description :   This method splits the dataframe into train set and test set based on split ratio 
        
        Output      :   Folder is created in s3 bucket
        On Failure  :   Write an exception log and then raise an exception
        """
        logging.info("Entered data split method of DataIngestion Component")
        try:
            train_set, test_set = train_test_split(dataframe, test_size=self.data_ingestion_config.train_test_split_ratio, random_state=100)
            logging.info("Performed train test split on dataframe")
            logging.info(
                "Exited data split method of DataIngestion Component"
            )

            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            create_directories([dir_path])
            logging.info(f"Exporting train & test data")
            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)

            logging.info("Exported train & test data")
        except Exception as e:
            raise UsVisaException(e, sys)
        
    
    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """
        Method Name :   initiate_data_ingestion
        Description :   This method initiates the data ingestion components of training pipeline 
        
        Output      :   train set and test set are returned as the artifacts of data ingestion components
        On Failure  :   Write an exception log and then raise an exception
        """
        logging.info("Entered initiate_data_ingestion method of DataIngestion Component")
        try:
            dataframe = self.export_data_into_feature_store()
            logging.info("Got the data from mongodb")
            self.split_data_as_train_test(dataframe)
            logging.info("Performed train_test_split on the data")
            logging.info("Exited initiate_data_ingestion method of DataIngestion Component")

            data_ingestion_artifact = DataIngestionArtifact(
                train_file_path = self.data_ingestion_config.training_file_path,
                test_file_path = self.data_ingestion_config.testing_file_path
            )

            logging.info(f"Data Ingestion Artifact: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise UsVisaException(e, sys)