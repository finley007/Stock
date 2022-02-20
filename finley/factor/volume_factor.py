#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd

import os,sys 
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir) 
from persistence import FileUtils
from visualization import draw_analysis_curve
from indicator import FI
from simulator import simulate 
from factor.base_factor import Factor
import tools

# 动量突破
class FIPenetration(Factor):
    
    factor_code = 'fi_penetration'
    version = '1.0'
    
    _high_limit = 0
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = FI(self._params)
        indicator.enrich(data)
        #只取穿透点
        data[FIPenetration.factor_code] = 0
        #向上突破标记线买入
        data.loc[(data['fi.'+str(self._params[0])].shift(1) < self._high_limit) & (data['fi.'+str(self._params[0])] > self._high_limit), FIPenetration.factor_code] = 100
        #向下突破标记线卖出
        data.loc[(data['fi.'+str(self._params[0])].shift(1) > self._high_limit) & (data['fi.'+str(self._params[0])] < self._high_limit), FIPenetration.factor_code] = -100
        return data 
    
     
if __name__ == '__main__':
    #图像分析
    data = FileUtils.get_file_by_ts_code('600438.SH', is_reversion = True)
    factor = FIPenetration([10])
    data = factor.caculate(data)
    data['index_trade_date'] = pd.to_datetime(data['trade_date'])
    data = data.set_index(['index_trade_date'])
    draw_analysis_curve(data[(data['trade_date'] <= '20220215') & (data['trade_date'] > '20210101')], volume = False, show_signal = True, signal_keys = ['SO.10','so_penetration'])
    print('aa')
    
    #模拟
    # data = FileUtils.get_file_by_ts_code('002531.SZ', is_reversion = False)
    # factor = RVIPenetration([10])
    # simulate(factor, data, start_date = '20210101', save = False)
    
    #计算两个因子相关性
    # data = FileUtils.get_file_by_ts_code('600256.SH', is_reversion = True)
    # factor = LowerHatch([5])
    # factor1 = MeanInflectionPoint([5])
    # print(factor.compare(factor1, data))
    
    # 计算并合并数据集
    # stock_list = ['002269.SZ','300939.SZ','600256.SH']
    # data_list = []
    # for ts_code in stock_list:
    #     data = FileUtils.get_file_by_ts_code(ts_code, is_reversion = True)
    #     data_list.append(data)
    # factor = LowerHatch([5])
    # data = factor.caculate_concat(data_list)
    # print(len(data))
    # print(data)
    