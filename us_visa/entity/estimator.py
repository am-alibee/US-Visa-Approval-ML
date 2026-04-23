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