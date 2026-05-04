import sys
import pandas as pd
from pandas import DataFrame

from us_visa.entity.config_entity import USvisaPredictorConfig
from us_visa.exception import UsVisaException
from us_visa.logger import logging

from us_visa.entity.s3_estimator import USVisaEstimator

class USvisaData:
    def __init__(
        self,
        continent,
        education_of_employee,
        has_job_experience,
        requires_job_training,
        no_of_employees,
        region_of_employment,
        prevailing_wage,
        unit_of_wage,
        full_time_position,
        yr_of_estab
    ):
        try:
            self.continent=continent
            self.education_of_employee=education_of_employee
            self.has_job_experience=has_job_experience
            self.requires_job_training=requires_job_training
            self.no_of_employees=no_of_employees
            self.region_of_employment=region_of_employment
            self.prevailing_wage=prevailing_wage
            self.unit_of_wage=unit_of_wage
            self.full_time_position=full_time_position
            self.yr_of_estab=yr_of_estab
        except Exception as e:
            raise UsVisaException(e, sys)
        
    def get_usvisa_data_as_dict(self):
        return {
            "continent": [self.continent],
            "education_of_employee": [self.education_of_employee],
            "has_job_experience": [self.has_job_experience],
            "requires_job_training": [self.requires_job_training],
            "no_of_employees": [self.no_of_employees],
            "region_of_employment": [self.region_of_employment],
            "prevailing_wage": [self.prevailing_wage],
            "unit_of_wage": [self.unit_of_wage],
            "full_time_position": [self.full_time_position],
            "yr_of_estab": [self.yr_of_estab]   
        }
    
    def get_usvisa_input_data_frame(self) -> DataFrame:
        try:
            return pd.DataFrame(self.get_usvisa_data_as_dict())
        except Exception as e:
            raise UsVisaException(e, sys)
        
class USvisaClassifier:
    def __init__(self, config: USvisaPredictorConfig = USvisaPredictorConfig()):
        try:
            self.config = config
            self.estimator = USVisaEstimator(
                bucket_name=config.model_bucket_name,
                model_path=config.model_file_path
            )
            logging.info("Loading deployed model and preprocessor")

        except Exception as e:
            raise UsVisaException(e, sys)
        
    def predict(self, dataframe: DataFrame):
        try:
            logging.info("Starting prediction using S3")

            # run prediction
            if not self.estimator.is_model_present():
                raise ValueError("No model found in S3. Train and deploy first")

            prediction = self.estimator.predict(dataframe)

            logging.info("Prediction completed successfully")

            return prediction
        
        except Exception as e:
            raise UsVisaException(e, sys)