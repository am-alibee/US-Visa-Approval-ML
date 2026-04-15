import sys
from us_visa.logger import logging
from us_visa.exception import UsVisaException

logging.info("Welcome to our custom log")

def divide():
    return 10 / 0

try:
    divide()
except Exception as e:
    raise UsVisaException(e, sys)

    # _, _, tb = sys.exc_info()

    # print("FILE: ", tb.tb_frame.f_code.co_filename)
    # print("LINE: ", tb.tb_lineno)