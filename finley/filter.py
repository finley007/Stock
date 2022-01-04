#! /usr/bin/env python
# -*- coding:utf8 -*-

"""
过滤器的参数都是动态参数，所以应用场景只是在选股的时候，
而不能用于模型训练，选股需要过滤的是原始数据而不是选择结果,
获取数据集最后一天的数据进行过滤计算
"""

from abc import ABCMeta, abstractclassmethod
import pandas as pd

import constants
from datasource import TushareDatasource
from persistence import DaoMysqlImpl, FileUtils

class Filter(metaclass = ABCMeta):
    
    _param = ''
    
    def __init__(self, param):
        self._param = param
        
    @abstractclassmethod
    def filter(self, data):
        pass
    
# 价格过滤
class PriceFilter(Filter):
        
    def filter(self, data):
        result =  data.iloc[len(data) - 1]['close'] < self._param  
        if (not result):
            print("stock: " + data["ts_code"] + "'s close price: " + data.iloc[len(data) - 1]['close'] +  " over the price filter threshold and will be filtered")
        return result
    
# ST标识过滤
class STFilter(Filter):
    _static_stock_list = []
    
    def __init__(self):
        self._param = ''
        if (len(self._static_stock_list) == 0):
            persistence = DaoMysqlImpl()
            static_stock_list = persistence.select("select ts_code, name from static_stock_list")
            self._static_stock_list = pd.DataFrame(static_stock_list, columns=['ts_code','name'])
            
    def filter(self, data):
        ts_code = data.iloc[len(data) - 1]['ts_code']
        if (len(self._static_stock_list[self._static_stock_list['ts_code'] == ts_code]) == 0):
            raise Exception("Stock {} not found in static_stock_list, need to be updated".format(ts_code))
        name = self._static_stock_list[self._static_stock_list['ts_code'] == ts_code]['name'].iloc[0]
        result = name.find('ST') == -1
        if (not result):
            print("stock: " + ts_code + " is ST stock and will be filterd")
        return result
    
# 市盈率过滤
class PERatioFilter(Filter):
    
    _indicator_data = []
    
    def __init__(self, param, trade_date):
        self._param = param
        if (len(self._indicator_data) == 0):
            datasource = TushareDatasource()
            self._indicator_data = datasource.daily_indicator(trade_date=trade_date)
            print(self._indicator_data)
    
    def filter(self, data):
        ts_code = data.iloc[len(data) - 1]['ts_code']
        result = True
        try:
            if (len(self._indicator_data) > 0 and not pd.isnull(self._indicator_data[self._indicator_data['ts_code'] == ts_code]['pe'].iloc[0])):
                result = self._indicator_data[self._indicator_data['ts_code'] == ts_code]['pe'].iloc[0] < self._param
            else:
                #获取不到市盈率，有可能是在亏损 
                result = False
            if (not result):
                print("stock: " + ts_code + "'s pe: " + str(self._indicator_data[self._indicator_data['ts_code'] == ts_code]['pe'].iloc[0]) +  " over the pe filter threshold and will be filtered")
        except Exception:
            #停牌
            result = False
        return result
    
# 次新股过滤
class NewStockFilter(Filter):
    
    def __init__(self, param):
        self._param = param
    
    def filter(self, data):
        return len(data) > int(self._param[0])
    
if __name__ == '__main__':
    data = FileUtils.load(constants.DATA_PATH + '688667.SH.pkl')
    data1 = FileUtils.load(constants.DATA_PATH + '600671.SH.pkl')
    trade_date = data.iloc[len(data) - 1]['trade_date']
    filter_list = []
    filter_list.append(PriceFilter(200))
    filter_list.append(STFilter())
    filter_list.append(PERatioFilter(50, trade_date))
    for filter in filter_list:
        print('aaaa' + str(filter.filter(data)))
        print('bbbb' + str(filter.filter(data1)))
        