#! /usr/bin/env python
# -*- coding:utf8 -*-

"""
这个文件主要用来做期货数据的分析，操作
1. 查看期货合约的日期时间区间
"""

import datetime
import time
import webbrowser

import constants
from persistence import FileUtils, DaoMysqlImpl
from visualization import draw_analysis_curve

#数据清洗
def clean_data(data):
    #去除空数据
    data = data.dropna()
    return data

#根据商品和合约获取可用数据的开始和结束时间
def get_datetime_range(product, instrument):
    data = FileUtils.get_file_by_product_and_instrument(product, instrument)
    data = clean_data(data)
    data['time'] = data.index
    start_time = data.idxmin()['time']
    end_time = data.idxmax()['time']
    return (start_time, end_time)

#根据商品获取合约列表
def get_instrument_info_by_product(product, inst_list = []):
    # 读取主力合约列表
    instrument_list = FileUtils.read_files_in_path(constants.FUTURE_DATA_PATH + product)
    instrument_list.sort()
    instrument_list = list(map(lambda str: str[0:str.index('-')], instrument_list))
    if (len(inst_list) > 0):
        instrument_list = list(filter(lambda str: str in inst_list, instrument_list))
    instrument_list = list(map(lambda instrument: create_instrument_info(product, instrument), instrument_list))    
    return instrument_list

def create_instrument_info(product, instrument):
    date_time_range = get_datetime_range(product, instrument)
    return [product, instrument, date_time_range[0], date_time_range[1]]

#获取全部产品
def init_all_product_instrument():
    persistence = DaoMysqlImpl()
    instrument_list = []
    product_list = FileUtils.read_files_in_path(constants.FUTURE_DATA_PATH)
    for product in product_list:
        # 排除全局文件，比如主力合约配置文件all-main.pkl等
        if len(product) > 3:
            continue
        # 读取主力合约列表
        instrument_list.extend(get_instrument_info_by_product(product)) 
    if (len(instrument_list) > 0):
        persistence.delete('delete from future_instrument_list')
        for instrument in instrument_list: 
            item = (instrument[0], instrument[1], instrument[2], instrument[3])
            persistence.insert('insert into future_instrument_list values (%s,%s,%s,%s)', [item])
            
#获取交易数据
def get_data_by_product_and_instrument(product, instrument, from_file = True):
    if (from_file):
        return FileUtils.get_file_by_product_and_instrument(product, instrument)
    else:
        persistence = DaoMysqlImpl()
        return persistence.get_future_kline_data(instrument) 

#获取合约的交易时间    
def get_transaction_time_range(product, instrument):
    data = FileUtils.get_file_by_product_and_instrument(product, instrument)
    data = clean_data(data)
    data['time'] = data.index
    start_time = data.idxmin()['time']
    print(start_time)
    
    
    
if __name__ == '__main__':
    # print(get_datetime_range('IF', 'IF1103'))
    # print(get_instrument_by_product('IF', ['IF1807','IF1809']))
    # print(get_instrument_info_by_product('IF'))
    # init_all_product_instrument()
    # data = get_data_by_product_and_instrument('RB', 'RB2210', False)
    # draw_analysis_curve(data, volume = True)
    get_transaction_time_range('IF', 'IF1103')
    print('aa')
    
        