#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'Sharath Samala'

# ################################# Module Information #################################################################
#   Module Name         : logsetup
#   Purpose             : Setup the environment and assign value to all the variables required for logging
#   Pre-requisites      : Config variables should be present in log_setup json file
#   Last changed on     : 28 May 2018
#   Last changed by     : Sharath Samala
#   Reason for change   : Development
# ######################################################################################################################

# Library and external modules declaration
import logging
import logging.handlers
import os
from datetime import datetime

MODULE_NAME = "Logsetup"
DEFAULT_LOG_FILE_NAME = "log_setup"
TIME_FORMAT = str(datetime.now().strftime("%Y%m%d_%H_%M_%S"))


def get_logger(file_name=None, append_ts=True, log_path='../logs/', timed_rotating_fh=False):
    console_logging_level = "DEBUG"
    file_logging_level = "DEBUG"

    # creating log file name based on the timestamp and file name
    if file_name is None:
        if append_ts:
            file_name = TIME_FORMAT
        else:
            file_name = DEFAULT_LOG_FILE_NAME
    else:
        if append_ts:
            file_name += "_" + TIME_FORMAT
    logger_obj = logging.getLogger(file_name)

    file_log_path = None

    if not logger_obj.handlers:
        # Set logging level across the logger. Set to INFO in production
        logger_obj.setLevel(logging.DEBUG)

        # create formatter
        log_formatter_keys = []
        if log_formatter_keys is not None:
            # if log_formatter_keys exists and its not a list or blank list default formatter will be created
            if len(log_formatter_keys) == 0 or not isinstance(log_formatter_keys, list):
                formatter_string = "%(asctime)s - %(levelname)s - %(message)s "
            else:
                # when log_formatter_keys in json contains keys, formatter using those keys will be created
                default_format_parameters = ["asctime", "levelname", "message"]
                default_format_parameters.extend(log_formatter_keys)
                formatter_string = "'"
                for keys in default_format_parameters:
                    formatter_string += "%(" + str(keys) + ")s - "
                formatter_string = formatter_string.rstrip(" - ")
                formatter_string += "'"
        else:
            # when log_formatter_keys does not exists default formatter will be created
            formatter_string = "%(asctime)s - %(levelname)s - %(message)s "

        formatter = logging.Formatter(formatter_string)

        # create file handler which logs even debug messages
        file_handler_flag = "N"
        if file_handler_flag == "Y":
            file_handler_path = log_path
            # checking if file_handler_path key is present in json
            if file_handler_path != "" and file_handler_path is not None:
                if os.path.exists(file_handler_path):
                    # if key is present and its correct path log file will be created on that path
                    file_log_path = os.path.join(str(file_handler_path), file_name)
                else:
                    file_log_path = file_name
            else:
                # log file will be created on path where code runs
                file_log_path = file_name

            file_log_path += '_file.log'
            file_handler = logging.FileHandler(file_log_path, delay=True)

            # Set logging level across the logger. Set to INFO in production
            if file_logging_level is not None:
                if file_logging_level.lower() == "debug":
                    file_handler.setLevel(logging.DEBUG)
                elif file_logging_level.lower() == "warning":
                    file_handler.setLevel(logging.WARNING)
                elif file_logging_level.lower() == "error":
                    file_handler.setLevel(logging.ERROR)
                else:
                    file_handler.setLevel(logging.INFO)
            else:
                file_handler.setLevel(logging.DEBUG)

            file_handler.setFormatter(formatter)

            logger_obj.addHandler(file_handler)


        # create console handler with debug level
        console_handler_flag = "Y"
        if console_handler_flag != "N":
            console_handler = logging.StreamHandler()

            # Set logging level across the logger. Set to INFO in production
            if console_logging_level is not None:
                if console_logging_level.lower() == "debug":
                    console_handler.setLevel(logging.DEBUG)
                elif console_logging_level.lower() == "warning":
                    console_handler.setLevel(logging.WARNING)
                elif console_logging_level.lower() == "error":
                    console_handler.setLevel(logging.ERROR)
                else:
                    console_handler.setLevel(logging.INFO)
            else:
                console_handler.setLevel(logging.DEBUG)

            console_handler.setFormatter(formatter)

            logger_obj.addHandler(console_handler)

    return logger_obj, file_log_path

logger, log_path = get_logger()
