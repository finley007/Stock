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
STOCK_DATA_PATH = DATA_PATH + 'stock/'
FUTURE_DATA_PATH = DATA_PATH + 'future/'
WORK_PATH = config.get('common', 'work_path')
TEMP_PATH = config.get('common', 'temp_path')

DB_TYPE = config.get('common', 'db_type')
DB_HOST = config.get('common', 'db_host')
DB_PORT = int(config.get('common', 'db_port'))
DB_USERNAME = config.get('common', 'db_username')
DB_PASSWORD = config.get('common', 'db_password')
DB_NAME = config.get('common', 'db_name')

ANALYSIS_RESULT_TOP_N = config.get('common', 'analysis_result_top_n')
TRANSACTION_CHARGE_RATE = config.get('common', 'transaction_charge_rate')

STOCK_INFO_LINK = config.get('sina', 'stock_info_link_template')

EXCLUDED_FILES = config.get('common', 'exclude_files').split(',')
