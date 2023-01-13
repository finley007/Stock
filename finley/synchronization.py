#! /usr/bin/env python
# -*- coding:utf8 -*-

import time
import datetime
import ray

import pandas as pd

from datasource import TushareDatasource
from persistence import DaoMysqlImpl, FileUtils

import constants

def synchronize_calendar():
    datasource = TushareDatasource()
    calendar_info = datasource.get_calendar()
    if not calendar_info.empty:
        persistence = DaoMysqlImpl()
        result = persistence.delete('delete from static_calendar')
        for index, row in calendar_info.iterrows():
            item = (index + 1, row[0], row[1], row[2])
            persistence.insert('insert into static_calendar values (%s,%s,%s,%s)', [item])
            
def synchronize_all_stock():
    datasource = TushareDatasource()
    stock_list = datasource.get_stock_list()
    if not stock_list.empty:
        persistence = DaoMysqlImpl()
        result = persistence.delete('delete from static_stock_list')
        for index, row in stock_list.iterrows():
            # item = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14])
            # persistence.insert('insert into static_stock_list_test values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [item])
            update_stock_info.remote(row, persistence)

@ray.remote  
def update_stock_info(row, persistence):
    item = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14])
    persistence.insert('insert into static_stock_list values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [item])
    
# 复权
def reverse_factor(ts_code, data, reversion_data):
    reversion_data.drop(columns = ['ts_code'], inplace = True)
    data = pd.merge(data, reversion_data, on = 'trade_date', how = 'left')
    data['adj_factor'].fillna(1, inplace = True)
    data['open'] = data['open'] * data['adj_factor']
    data['close'] = data['close'] * data['adj_factor']
    data['high'] = data['high'] * data['adj_factor']
    data['low'] = data['low'] * data['adj_factor']
    return data

# 生成股票日交易数据
def synchronize_stock_daily_data(ts_code, start_date = '19890101', is_reversion = False):
    end_date = time.strftime("%Y%m%d", time.localtime())
    datasource = TushareDatasource()
    data = datasource.daily_quotes(ts_code=ts_code, start_date=start_date, end_date=end_date)
    total_data = data
    while (len(data) == 5000):
        end_date = data.loc[4999]['trade_date']
        end_date = get_next_n_day(end_date, -1)
        data = datasource.daily_quotes(ts_code=ts_code, start_date=start_date, end_date=end_date)
        total_data = pd.concat([total_data, data])
        total_data = total_data.reset_index(drop=True)
    if (is_reversion):
        reversion_data = datasource.reverse_factor(ts_code)
        total_data = reverse_factor(ts_code, total_data, reversion_data)
    total_data = total_data.iloc[::-1]
    total_data = total_data.reset_index(drop=True)
    return total_data

def get_next_n_day(current_date, n):
    current_date = datetime.datetime.strptime(current_date, "%Y%m%d")
    current_date = current_date + datetime.timedelta(days=n)
    return current_date.strftime("%Y%m%d")
   
# 全量生成股票日交易数据     
def synchronize_all_stock_daily_data(is_reversion = False):
    persistence = DaoMysqlImpl()
    # stock_list = persistence.select("select ts_code from static_stock_list where ts_code > '002357.SZ'")
    stock_list = persistence.select("select ts_code from static_stock_list")
    for ts_code in stock_list:
        data = synchronize_stock_daily_data(ts_code[0], is_reversion = is_reversion)
        print('Sync stock: ' + ts_code[0])
        FileUtils.save_file_by_ts_code(data, ts_code[0], is_reversion)

def incremental_synchronize_stock_daily_data(ts_code, is_reversion = False):
    try:
        data = FileUtils.get_file_by_ts_code(ts_code, is_reversion = is_reversion)
        data = data.dropna()
        latest_date = data['trade_date'].max()
        current_date = time.strftime("%Y-%m-%d", time.localtime())
        if (latest_date != current_date):
            datasource = TushareDatasource()
            latest_date = latest_date.replace('-','')
            latest_date = get_next_n_day(latest_date, 1)
            current_date = current_date.replace('-','')
            new_data = datasource.daily_quotes(ts_code=ts_code, start_date=latest_date, end_date=current_date)
            if (is_reversion):
                reversion_data = datasource.reverse_factor(ts_code)
                new_data = reverse_factor(ts_code, new_data, reversion_data)
            new_data = new_data.iloc[::-1]
            new_data = new_data.reset_index(drop=True)
            total_data = pd.concat([data, new_data])
            total_data = total_data.reset_index(drop=True)
            data = total_data
    except Exception:
        data = synchronize_stock_daily_data(ts_code=ts_code)
    return data
    
def incremental_synchronize_all_stock_daily_data(is_reversion = False):
    persistence = DaoMysqlImpl()
    # stock_list = persistence.select("select ts_code from static_stock_list")
    stock_list = persistence.select("select ts_code from static_stock_list where ts_code >= '603305.SH'")
    for ts_code in stock_list:
        data = incremental_synchronize_stock_daily_data(ts_code[0], is_reversion)
        print('Sync stock: ' + ts_code[0])
        FileUtils.save_file_by_ts_code(data, ts_code[0], is_reversion)

#获取新股 
def get_new_stock(days = 7):
    result = []
    synchronize_all_stock()
    persistence = DaoMysqlImpl()
    stock_list = persistence.select("select ts_code from static_stock_list")
    datasource = TushareDatasource()
    for stock in stock_list:
        data = datasource.daily_quotes(ts_code = stock[0])
        if (len(data) <= days):
            result.append(stock)
    return result

if __name__ == '__main__':
    # print(time.strftime("%Y%m%d", time.localtime()))
    # print(get_next_n_day('20000101', -1))
    synchronize_calendar()
    # synchronize_all_stock()
    # 不复权
    # synchronize_all_stock_daily_data()
    # 复权
    # synchronize_all_stock_daily_data(is_reversion = True)
    # 手动同步
    # data = synchronize_stock_daily_data('002001.SZ', is_reversion = True)
    # FileUtils.save_file_by_ts_code(data, '002001.SZ', is_reversion = True)
    # 手动增量同步
    # data = incremental_synchronize_stock_daily_data('002304.SZ', is_reversion = True)
    # FileUtils.save_file_by_ts_code(data, '002304.SZ', is_reversion = True)
    # 增量同步不复权
    # incremental_synchronize_all_stock_daily_data()
    # 增量同步复权
    # incremental_synchronize_all_stock_daily_data(is_reversion = True)
    # print(get_new_stock(10))