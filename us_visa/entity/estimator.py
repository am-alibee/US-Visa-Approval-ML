import sys
import pandas as pd
from typing import Dict

from sklearn.pipeline import Pipeline

from us_visa.logger import logging
from us_visa.exception import UsVisaException


# Target value mapping
class TargetValueMapping:
    def __init__(self):
        self.mapping = {
            "Certified": 0,
            "Denied": 1
        }

    def to_numeric(self) -> Dict[str, int]:
        return self.mapping
    
    def to_category(self) -> Dict[int, str]:
        return {v: k for k,v in self.mapping.items()}
    
# model wrapper
class USVisaModel:
    def __init__(self, preprocessing_object: Pipeline, trained_model_object):
        self.preprocessing = preprocessing_object
        self.model = trained_model_object

    
    # input validation
    def _validate_input(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a DataFrame")
        
        if df.empty:
            raise ValueError("Input DataFrame is empty")
        
    # prediction
    def predict(self, df: pd.DataFrame):
        try:
            self._validate_input(df)

            transformed = self.preprocessing.transform(df)
            preds = self.model.predict(transformed)

            return pd.Series(preds)
        except Exception as e:
            raise UsVisaException(e, sys)
        
    def predict_proba(self, df: pd.DataFrame):
        try:
            self._validate_input(df)
            transformed = self.preprocessing.transform(df)

            if hasattr(self.model, "predict_proba"):
                return self.model.predict_proba(transformed)

            raise AttributeError("Model does not support predict_proba")
        except Exception as e:
            raise UsVisaException(e, sys)
        
    # debug representation
    def __repr__(self):
        return f"USVisaModel(model={type(self.model).__name__})"
    
    def __str__(self):
        return f"USVisaModel(model={type(self.model).__name__})"
    
