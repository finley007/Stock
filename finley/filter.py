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
from persistence import DaoMysqlImpl, FileUtils, create_session, SectionStockMapping
from tools import create_instance, to_params

class Filter(metaclass = ABCMeta):
    
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
            print("stock: " + data.iloc[len(data) - 1]["ts_code"] + "'s close price: " + str(data.iloc[len(data) - 1]['close']) +  " over the price filter threshold and will be filtered")
        return result
    
# ST标识过滤
class STFilter(Filter):
    
    def __init__(self):
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
    
    def __init__(self, param, trade_date):
        self._param = param
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
        return len(data) > int(self._param)
    
# 板块过滤
class SectionFilter(Filter):
    
    def __init__(self, params):
        session = create_session()
        section_stocking_mapping_list = session.query(SectionStockMapping).filter(SectionStockMapping.section_code.in_(params)).all()
        self._stock_set = set(list(map(lambda item : item.ts_code, section_stocking_mapping_list)))
        
    def filter(self, data):
        stock = data.iloc[len(data) - 1]['ts_code']
        return stock in self._stock_set
    
# 筛选
def filter_stock(filter_list, data):
    if (len(filter_list) == 0):
        return True
    for filter in filter_list:
        if (not filter.filter(data)):
            return False
    return True
    
def create_filter_list(filters=''):
    filter_list = []
    if filters == '':
        return filter_list
    filter_exp_list = filters.split('|')
    if len(filter_exp_list) > 0:
        for filter in filter_exp_list:
            filter_with_param = filter.split('_')
            if len(filter_with_param) > 1:
                filter_list.append(create_instance('filter', filter_with_param[0], to_params(filter_with_param[1])))
            else:
                filter_list.append(create_instance('filter', filter_with_param[0]))
    return filter_list

def to_params(str):
    if (str.find("[") != -1 and str.find("]") != -1):
        params = list(map(lambda str: type_conversion(str), str.split("-")))
        return params
    else:
        return type_conversion(str)
    
def type_conversion(str):
    try:
        return int(str)
    except ValueError:
        try:
            return float(str)
        except ValueError:
            return str

if __name__ == '__main__':
    data = FileUtils.load(constants.DATA_PATH + '/stock/688667.SH.pkl')
    data1 = FileUtils.load(constants.DATA_PATH + '/stock/600671.SH.pkl')
    trade_date = data.iloc[len(data) - 1]['trade_date']
    filter_list = []
    filter_list.append(PriceFilter(200))
    filter_list.append(STFilter())
    filter_list.append(PERatioFilter(50, trade_date))
    filter_list.append(SectionFilter(['BK0481']))
    for filter in filter_list:
        print('aaaa' + str(filter.filter(data)))
        print('bbbb' + str(filter.filter(data1)))
        
    filter_list1 = create_filter_list('PriceFilter_200|STFilter|SectionFilter_[BK0481]')
    for filter in filter_list1:
        print('aaaa' + str(filter.filter(data)))
        print('bbbb' + str(filter.filter(data1)))
        