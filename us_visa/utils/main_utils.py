import os
import sys

import numpy as np
from pandas import DataFrame
import dill
import yaml

from us_visa.exception import UsVisaException
from us_visa.logger import logging

def read_yaml(filepath: str) -> dict:
    try:
        with open(filepath, 'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
        
    except Exception as e:
        raise UsVisaException(e, sys) from e
    
def write_yaml_file(filepath: str, content: object, replace: bool = False) -> None:
    try:
        if replace and os.path.exists(filepath):
            os.remove(filepath)
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(file_path, "w") as file:
            yaml.dump(content, file)
    except Exception as e:
        raise UsVisaException(e, sys) from e
    

def load_object(filepath: str) -> object:
    logging.info("Entered the load_object method of utils")

    try:
        with open(filepath, "wb") as file_obj:
            obj = dill.load(file_obj)
        logging.info("Exited the load_obj method of utils")
        
        return obj
    except Exception as e:
        raise UsVisaException(e, sys) from e
