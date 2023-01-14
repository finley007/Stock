#! /usr/bin/env python
# -*- coding:utf8 -*-

from abc import ABCMeta, abstractclassmethod
import pandas as pd
# import ray
import time

from persistence import DaoMysqlImpl, FileUtils
from datasource import TushareDatasource
import tools
from synchronization import incremental_synchronize_stock_daily_data, synchronize_all_stock, synchronize_stock_daily_data

class DataException(Exception):
    
    def __init__(self, msg):
         self.message = msg
         
    def __str__(self):
        return self.message
    
     
# 校验类基类
class DataValidator(metaclass = ABCMeta):
    
    @abstractclassmethod
    def validate(self, data):
        pass

# 日期校验
class DateValidator(DataValidator):
    
    def validate(self, data):
        return data['trade_date'].is_monotonic
    
# 对比数据库和daily_indicator接口的股票
def validate_db_api_stock():
    datasource = TushareDatasource()
    api_stock_list = datasource.daily_indicator(trade_date = tools.get_current_date())
    persistence = DaoMysqlImpl()
    db_stock_list = persistence.select("select ts_code from static_stock_list")
    db_stock_list1 = []
    for stock in db_stock_list:
        db_stock_list1.append(stock[0])
    stock_list = api_stock_list[~api_stock_list['ts_code'].isin(db_stock_list1)]

def validate_data_integrity():
    persistence = DaoMysqlImpl()
    stock_list = persistence.select("select ts_code from static_stock_list")
    for stock in stock_list:
        # if stock[0] == '':
        validate_stock(stock[0], persistence)
    # time.sleep(300)

# @ray.remote  
def validate_stock(ts_code, persistence):
    try:
        data = FileUtils.get_file_by_ts_code(ts_code, is_reversion = True)
        if (len(data) == 0):
            raise DataException("%s data is empty"%(ts_code))
        if (data.isnull().values.any()):
            raise DataException("%s data has null value"%(ts_code))
        last_business_date = persistence.get_last_business_date()
        if (data['trade_date'].max() != last_business_date):
            raise DataException("%s data not update"%(ts_code))
    except (FileNotFoundError, DataException) as e:
        print('Incremetal sync stock: ' + ts_code)
        data = incremental_synchronize_stock_daily_data(ts_code, is_reversion = False)
        FileUtils.save_file_by_ts_code(data, ts_code, is_reversion = False)
        data = incremental_synchronize_stock_daily_data(ts_code, is_reversion = True)
        FileUtils.save_file_by_ts_code(data, ts_code, is_reversion = True)
    

if __name__ == '__main__':
    # 对比数据库和daily_indicator接口的股票
    # validate_db_api_stock()
    
    # 测试验证
    data = FileUtils.get_file_by_ts_code('300816.SZ', is_reversion = True)
    validator = DateValidator()
    print(validator.validate(data))