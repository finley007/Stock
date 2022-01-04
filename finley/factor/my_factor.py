#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd

import os,sys 
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir)  
from visualization import draw_analysis_curve
from persistence import DaoMysqlImpl, FileUtils
from factor.base_factor import Factor

'''
过去n天上涨比例因子
''' 
class RisingTrend(Factor):
    
    def __init__(self, params):
        self._params = params
        self._factor_code = 'rising_trend'
        
    def score(self, data):
        #只计算最后一天的因子，为了减小计算量取计算最后一天因子值所需的最小数据集
        data = data.loc[len(data)-self._params[0]:len(data)-1]
        data = self.caculate(data)
        #最近一天的最小参数
        return data.iloc[len(data) - 1][self._factor_code + str(self._params[0])]
        
    def caculate(self, data):
        if self._params:
            for param in self._params:
                data["increase"] = data["close"] - data["open"]
                data[self._factor_code + str(param)] = data.rolling(window = param)["increase"].apply(lambda x: self.get_rising_rate(x), raw=False)
        return data
    
    def get_rising_rate(self, x):
        return len(x[x > 0]) / len(x)
 
'''
过去n天下跌比例因子
'''    
class FallingTrend(Factor):
    
    def __init__(self, params):
        self._params = params
        self._factor_code = 'falling_trend'
    
    def score(self, data):
        data = data.loc[len(data)-self._params[0]:len(data)-1]
        data = self.caculate(data)
        #最近一天的最小参数
        return data.iloc[len(data) - 1][self._factor_code + str(self._params[0])]
    
    def caculate(self, data):
        if self._params:
            for param in self._params:
                data["reduce"] = data["close"] - data["open"]
                data[self._factor_code + str(param)] = data.rolling(window = param)["reduce"].apply(lambda x: self.get_rising_rate(x), raw=False);
        return data
    
    def get_rising_rate(self, x):
        return len(x[x < 0]) / len(x)
    
   
'''
带下长影线的局部低点
'''   
class LowerHatch(Factor):
    
    def __init__(self, params):
        self._params = params
        self._factor_code = 'lower_hatch'
    
    def score(self, data):
        data = data.loc[len(data)-self._params[0]:len(data)-1]
        data = self.caculate(data)
        #最近一天的最小参数
        return data.iloc[len(data) - 1][self._factor_code]
    
    def caculate(self, data):
        # 计算下影线比例
        data.loc[data["open"] >= data["close"], 'len_lower_hatch'] = data["close"] - data["low"]
        data.loc[data["open"] < data["close"], 'len_lower_hatch'] = data["open"] - data["low"]
        data['amplitude'] = data["high"] - data["low"]
        data["lower_hatch_scale"] = data['len_lower_hatch'] / data["amplitude"]
        #下降幅度，参数范围内最高值到当日收盘价的降幅
        data['decline'] = ((data['high'].rolling(window = self._params[0]).max() - data['close'])/data['close']) * pow(0.9, self._params[0])/(1 - pow(0.9, self._params[0]))
        data[self._factor_code] = (data["lower_hatch_scale"] + 3 * data['decline'])/4
        return data
    
    def get_action_mapping(self, item):
        if (item[self._factor_code] > 0.5):
            return 1
        elif (item[self._factor_code] < 0.1):
            return -1
        else:
            return 0
        
if __name__ == '__main__':
    #图像分析
    data = FileUtils.get_file_by_ts_code('002454.SZ', is_reversion = True)
    factor = LowerHatch([5])
    data = factor.caculate(data)
    data['index_trade_date'] = pd.to_datetime(data['trade_date'])
    data = data.set_index(['index_trade_date'])
    draw_analysis_curve(data[(data['trade_date'] <= '20211112') & (data['trade_date'] > '20210101')], volume = False, show_signal = True, signal_keys = [factor.get_factor_code()])
    print('aa')
    # print(factor.score(data))
    
    #模拟
    # data = FileUtils.get_file_by_ts_code('002454.SZ', is_reversion = False)
    # # factor = LowerHatch([5])
    # factor = MeanInflectionPoint([20])
    # # # factor = MeanPenetration([20])
    # # # factor = EnvelopePenetration_MeanPercentage([20])
    # # # factor = EnvelopePenetration_ATR([20])
    # # # factor = MACDPenetration([])
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