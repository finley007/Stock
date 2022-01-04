#! /usr/bin/env python
# -*- coding:utf8 -*-

from abc import ABCMeta, abstractclassmethod
from retry import retry

import pandas as pd
import logging

import tushare as ts

class Datasource(metaclass = ABCMeta):
    
    #静态数据部分
    @abstractclassmethod
    def get_stock_list(self):
        pass
    
    @abstractclassmethod
    def get_calendar(self, exchange='', start_date='', end_date='', is_open=''):
        pass
    
    #实时数据部分
    @abstractclassmethod
    def daily_quotes(self, ts_code, trade_date='', start_date='', end_date=''):
        pass
    
    @abstractclassmethod
    def daily_indicator(self, ts_code, trade_date='', start_date='', end_date=''):
        pass
    

class TushareDatasource(Datasource):
    
    def __init__(self):
        self.__token = '0c143eb70cae0d402803ea08ae1a05251928a82dc5a7274db069cb14'
        ts.set_token(self.__token)
        self.__pro = ts.pro_api()
        
    def get_stock_list(self):
        return self.__pro.stock_basic(fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
    
    def get_calendar(self, exchange='', start_date='', end_date='', is_open=''):
        return self.__pro.trade_cal(exchange = exchange, start_date = start_date, end_date = end_date, is_open = is_open)
    
    @retry(tries=3)
    def daily_quotes(self, ts_code, trade_date='', start_date='', end_date=''):
        return self.__pro.daily(ts_code = ts_code, trade_date = trade_date, start_date = start_date, end_date = end_date)
    
    def daily_indicator(self, ts_code='', trade_date='', start_date='', end_date=''):
        return self.__pro.daily_basic(ts_code = ts_code, trade_date = trade_date, start_date = start_date, end_date = end_date)
    
    @retry(tries=3)
    def reverse_factor(self, ts_code, trade_date=''):
        return self.__pro.adj_factor(ts_code = ts_code, trade_date = trade_date)
    
if __name__ == '__main__':
    datasource = TushareDatasource()
    # print(datasource.get_stock_list())
    # print(datasource.get_calendar(start_date='20200101', end_date='20211231'))
    # print(datasource.daily_quotes('000001.SZ',start_date='19910101',end_date='20210801'))
    # print(datasource.daily_indicator(trade_date='20210830'))
    print(datasource.restoration_factor(ts_code = '603882.SH'))
    
    