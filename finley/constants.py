#! /usr/bin/env python
# -*- coding:utf8 -*-

import os
from config import Config
import logging

config = Config()

LOG_PATH = config.get('common', 'log_path')
LOG_LEVEL = config.get('common', 'log_level')
LOG_FILE = config.get('common', 'log_file')
logging.basicConfig(filename=LOG_PATH + '/' + LOG_FILE, level=LOG_LEVEL)

DATA_PATH = config.get('common', 'data_path')
WORK_PATH = config.get('common', 'work_path')
TEMP_PATH = config.get('common', 'temp_path')

DB_HOST = config.get('common', 'db_host')
DB_USERNAME = config.get('common', 'db_username')
DB_PASSWORD = config.get('common', 'db_password')
DB_NAME = config.get('common', 'db_name')

ANALYSIS_RESULT_TOP_N = config.get('common', 'analysis_result_top_n')

STOCK_INFO_LINK = config.get('sina', 'stock_info_link_template')
