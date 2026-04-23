import os
import sys

import numpy as np
from pandas import DataFrame
import dill
import yaml

from us_visa.exception import UsVisaException
from us_visa.logger import logging

def create_directories(directory_path: list) -> None:
    try:
        for path in directory_path:
            os.makedirs(path, exist_ok=True)
            logging.info(f"New directory created at {path}")
    except Exception as e:
        logging.error(f"Cannot Create directories")
        raise UsVisaException(e, sys) from e


def read_yaml(filepath: str) -> dict:
    try:
        with open(filepath, 'r') as yaml_file:
            return yaml.safe_load(yaml_file)
        
    except Exception as e:
        raise UsVisaException(e, sys) from e
    
def write_yaml_file(filepath: str, content: object, replace: bool = False) -> None:
    try:
        if replace and os.path.exists(filepath):
            os.remove(filepath)
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as file:
            yaml.dump(content, file)
    except Exception as e:
        raise UsVisaException(e, sys) from e
    
def save_object(file_path: str, obj: object) -> None:
    logging.info("Entered the save_object method of utils")

    try:
        dir_path = os.path.dirname(file_path)
        create_directories([dir_path])
        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)
        
        logging.info("Exiting the save_object method of utils")

    except Exception as e:
        raise UsVisaException(e, sys) from e
    


def load_object(filepath: str) -> object:
    logging.info("Entered the load_object method of utils")

    try:
        with open(filepath, "rb") as file_obj:
            obj = dill.load(file_obj)
        logging.info("Exited the load_obj method of utils")
        
        return obj
    except Exception as e:
        raise UsVisaException(e, sys) from e


def save_numpy_array_data(file_path: str, array: np.array):
    """
    Save numpy array data to file
    file_path: str: location of file to save
    array: np.array data to save
    """
    try:
        dir_path = os.path.dirname(file_path)
        create_directories([file_path])
        with open(file_path, "wb") as file_obj:
            np.save(file_obj, array)
    except Exception as e:
        raise UsVisaException(e, sys) from e
    

def load_numpy_array_data(file_path: str) -> np.array:
    """
    Load numpy array from file
    file_path: str location of the file to load
    return np.array: data loaded
    """
    try:
        with open(file_path, "rb") as file_obj:
            return np.load(file_obj)
    except Exception as e:
        raise UsVisaException(e, sys) from e
    

def drop_columns(df: DataFrame, cols: list) -> DataFrame:
    """
    Drop the provided cols from a pandas DataFrame
    df: pandas DataFram
    cols: list of columns to be dropped
    """
    logging.info("Entered drop_columns method of utils")

    try:
        df = df.drop(columns=cols, axis=1)
        logging.info("Exited the drop_columns method of utils")

        return df
    except Exception as e:
        raise UsVisaException(e, sys)
    
    