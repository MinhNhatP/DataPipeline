import os
import copy
import datetime
from datetime import timedelta
import traceback

from util import extractor_config
from util.generate_log_url import logging_url

DATETIME_FORMAT = os.getenv('DATETIME_FORMAT')
MINIMUM_DELTA_TIME = int(os.getenv('MINIMUM_DELTA_TIME'))

def get_extraction_configuration(event):
        if 'object' in event:
            data_extractor_config = data_extractor_config.get_config_for_all_object()
        else:
            list_objects =list(event["object"])
            data_extractor_config = extractor_config.get_config_for_a_list_of_objects(list_objects)
        return data_extractor_config