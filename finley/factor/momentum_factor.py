#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd

import os,sys 
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir) 
from persistence import FileUtils
from visualization import draw_analysis_curve
from indicator import MACD, MovingAverage, DIEnvelope, RSI, KDJ, DRF
from simulator import simulate 
from factor.base_factor import Factor

# 动量突破
class MomentumPenetration(Factor):
    
    def __init__(self, params):
        self._params = params
        self._factor_code = 'momentum_penetration'
        self._version = '1.0'
    
    def caculate(self, data):
        data['momentum.' + str(self._params[0])] = data['close'] - data['close'].shift(self._params[0])
        #只取穿透点
        data[self._factor_code] = 0
        data.loc[(data['momentum.' + str(self._params[0])].shift(1) < 0) & (data['momentum.' + str(self._params[0])] > 0), self._factor_code] = data['momentum.' + str(self._params[0])] - data['momentum.' + str(self._params[0])].shift(1)
        data.loc[(data['momentum.' + str(self._params[0])].shift(1) > 0) & (data['momentum.' + str(self._params[0])] < 0), self._factor_code] = data['momentum.' + str(self._params[0])] - data['momentum.' + str(self._params[0])].shift(1)
        return data 
    
# 动量回归
class MomentumRegression(Factor):
    
    def __init__(self, params):
        self._params = params
        self._factor_code = 'momentum_regression'
        self._version = '1.0'
    
    def caculate(self, data):
        indicator = MovingAverage([self._params[0]])
        data = indicator.enrich(data)
        data['momentum.' + str(self._params[0])] = (data['close'] - data['mean.'+str(self._params[0])])/data['mean.'+str(self._params[0])]
        #只取穿透点
        data[self._factor_code] = 0
        return data 

# 离散指标
class DiscreteIndex(Factor):
    
    def __init__(self, params):
        self._params = params
        self._factor_code = 'discrete_index'
        self._version = '1.0'
    
    def caculate(self, data):
        indicator = DIEnvelope([self._params])
        data = indicator.enrich(data)
        #只取穿透点
        data[self._factor_code] = 0
        return data       

# MACD
class MACDPenetration(Factor):
    
    def __init__(self, params):
        self._params = params
        self._factor_code = 'macd_penetration'
        self._version = '1.0'
    
    def caculate(self, data):
        indicator = MACD(self._params)
        data = indicator.enrich(data)
        data['prev_gap'] = data['DIFF'].shift(1) - data['DEA'].shift(1)
        data['cur_gap'] = data['DIFF'] - data['DEA']
        data = data.dropna()
        data[self._factor_code] = 0
        #金叉
        data.loc[(data['prev_gap'] < 0) & (data['cur_gap'] > 0) & (data['DIFF'] > 0), self._factor_code] = abs(data['prev_gap']) + abs(data['cur_gap'])
        #死叉
        data.loc[(data['prev_gap'] > 0) & (data['cur_gap'] < 0) & (data['DIFF'] > 0), self._factor_code] = -(abs(data['prev_gap']) + abs(data['cur_gap']))
        return data  
    
   
# RSI突破
class RSIPenetration(Factor):
    
    _high_limit = 70
    _low_limit = 30
    
    def __init__(self, params):
        self._params = params
        self._factor_code = 'rsi_penetration'
        self._version = '1.0'
    
    def caculate(self, data):
        indicator = RSI([self._params[0]])
        data = indicator.enrich(data)
        #只取穿透点
        data[self._factor_code] = 0
        #突破下限买入
        data.loc[(data['rsi.' + str(self._params[0])].shift(1) > self._low_limit) & (data['rsi.' + str(self._params[0])] < self._low_limit), self._factor_code] = 1
        #突破上限卖出
        data.loc[(data['rsi.' + str(self._params[0])].shift(1) < self._high_limit) & (data['rsi.' + str(self._params[0])] > self._high_limit), self._factor_code] = -1
        return data 
    
# DRF突破
class DRFPenetration(Factor):
    
    _high_limit = 0.7
    _low_limit = 0.3
    
    def __init__(self, params):
        self._params = params
        self._factor_code = 'drf_penetration'
        self._version = '1.0'
    
    def caculate(self, data):
        indicator = DRF([self._params[0]])
        data = indicator.enrich(data)
        #只取穿透点
        data[self._factor_code] = 0
        #突破下限买入
        data.loc[(data['DRF.' + str(self._params[0])].shift(1) > self._low_limit) & (data['DRF.' + str(self._params[0])] < self._low_limit), self._factor_code] = 1
        #突破上限卖出
        data.loc[(data['DRF.' + str(self._params[0])].shift(1) < self._high_limit) & (data['DRF.' + str(self._params[0])] > self._high_limit), self._factor_code] = -1
        return data 
    
# KDJ回归
class KDJRegression(Factor):
    
    _high_limit = 80
    _low_limit = 20
    
    def __init__(self, params):
        self._params = params
        self._factor_code = 'kdj_regression'
        self._version = '1.0'
    
    def caculate(self, data):
        indicator = KDJ([self._params[0]])
        data = indicator.enrich(data)
        #只取穿透点
        data[self._factor_code] = 0
        #突破下限买入
        data.loc[(data['D.' + str(self._params[0])].shift(1) > self._low_limit) & (data['D.' + str(self._params[0])] < self._low_limit), self._factor_code] = 1
        #突破上限卖出
        data.loc[(data['D.' + str(self._params[0])].shift(1) < self._high_limit) & (data['D.' + str(self._params[0])] > self._high_limit), self._factor_code] = -1
        return data 
     
if __name__ == '__main__':
    #图像分析
    # data = FileUtils.get_file_by_ts_code('002667.SZ', is_reversion = True)
    # # # factor = MACDPenetration([])
    # # # factor = MomentumPenetration([20])
    # # factor = MomentumRegression([10])
    # # # factor = DiscreteIndex([10, 40])
    # # factor = KDJRegression([9])
    # factor = DRFPenetration([0.3])
    # data = factor.caculate(data)
    # data['index_trade_date'] = pd.to_datetime(data['trade_date'])
    # data = data.set_index(['index_trade_date'])
    # draw_analysis_curve(data[(data['trade_date'] <= '20211231') & (data['trade_date'] > '20210101')], volume = False, show_signal = True, signal_keys = ['DRF','DRF.0.3','drf_penetration'])
    # print('aa')
    # print(factor.score(data))
    
    #模拟
    data = FileUtils.get_file_by_ts_code('002667.SZ', is_reversion = False)
    # factor = LowerHatch([5])
    # factor = MeanInflectionPoint([20])
    # factor = MeanPenetration([20])
    # factor = EnvelopePenetration_MeanPercentage([20])
    # factor = EnvelopePenetration_ATR([20])
    # factor = MACDPenetration([])
    # factor = KDJRegression([9])
    factor = DRFPenetration([0.3])
    simulate(factor, data, start_date = '20210101', save = False)
    
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