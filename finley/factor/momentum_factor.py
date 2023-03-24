#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd
import numpy as np

import os,sys 
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir) 
from persistence import FileUtils
from visualization import draw_analysis_curve
from indicator import MACD, MovingAverage, DIEnvelope, RSI, KDJ, DRF, WR, UO, RVI, SO, DI
from simulator import simulate 
from factor.base_factor import Factor, CombinedParamFactor
import tools

# 动量突破
class MomentumPenetration(Factor):
    
    factor_code = 'momentum_penetration'
    version = '1.0'
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data, create_signal = True):
        for param in self._params:
            data[self.get_key(param)] = data['close'] - data['close'].shift(param)
            if create_signal:
                data[self.get_signal(param)] = data[self.get_key(param)].rolling(2).apply(lambda item: self.get_action_mapping(param, item))
        return data 
    
    def get_action_mapping(self, param, item):
        key_list = item.tolist()
        if key_list[0] < 0 and key_list[1] > 0:
            return 1
        else:
            return 0
    
# 动量回归
class MomentumRegression(Factor):
    
    factor_code = 'momentum_regression'
    version = '1.0'
        
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data, create_signal = True):
        indicator = MovingAverage(self._params)
        data = indicator.enrich(data)
        for param in self._params: 
            data[self.get_key(param)] = (data['close'] - data[indicator.get_key(param)]) / data[indicator.get_key(param)]
            if create_signal:
                data[self.get_signal(param)] = data.apply(lambda item: self.get_action_mapping(param, item), axis = 1)
        return data 
    
    def get_action_mapping(self, param, item):
        if (item[self.get_key(param)] < -0.1):
            return 1
        else:
            return 0

# 离散指标
class DiscreteIndex(CombinedParamFactor):
    
    factor_code = 'discrete_index'
    version = '1.0'
        
    def __init__(self, params):
        self._params = params
        
    def get_high_value_key(self):
        return self.factor_code + '.high.' + str(self._params[0]) + '.' + str(self._params[1])
    
    def get_middle_value_key(self):
        return self.factor_code + '.middle.' + str(self._params[0]) + '.' + str(self._params[1])
    
    def caculate(self, data, create_signal = True):
        indicator = DIEnvelope(self._params)
        data = indicator.enrich(data)
        data[self.get_middle_value_key()] = data[indicator.get_middle_value_key()]
        data[self.get_high_value_key()] = data[indicator.get_high_value_key()]
        data[self.get_key()] = data[self.get_middle_value_key()] - data[self.get_high_value_key()]
        if create_signal:
            data[self.get_signal()] = data[[self.get_key()]].rolling(2).apply(lambda item: self.get_action_mapping(item))
        return data  
    
    def get_action_mapping(self, item):
        key_list = item.tolist()
        if key_list[0] < 0 and key_list[1] > 0:
            return 1
        else:
            return 0   

    def get_factor_value_list(self, param, sub_list, start_date = '', end_date = ''):
        for stock in sub_list:
            print('Handle stock: ' + stock)
            data = FileUtils.get_file_by_ts_code(stock)
            data = self.caculate(data)
            data = data.dropna()
            if start_date != '':
                data = data[data['date'] >= start_date]
            if end_date != '':
                data = data[data['date'] <= start_date]
        return data[self.get_key()].tolist()

# MACD
class MACDPenetration(CombinedParamFactor):
    
    factor_code = 'macd_penetration'
    version = '1.0'
        
    def __init__(self, params=[12, 26, 9]):
        self._params = params
        
    def caculate(self, data, create_signal=True):
        indicator = MACD(self._params)
        data = indicator.enrich(data)
        data[self.get_key()] = data[indicator.get_key()]
        data[MACDPenetration.factor_code] = 0
        if create_signal:
            data['cross'] = data[[self.get_key()]].rolling(2).apply(lambda item: self.get_action_mapping(item))
            # 金叉开仓
            data.loc[(data['cross'] == 1) & (data['DIFF'] > 0) & (data['DEA'] > 0), self.get_signal()] = 1
            # 死叉平仓
            data.loc[(data['cross'] == -1), self.get_signal()] = 1
        return data  
    
    def get_action_mapping(self, item):
        key_list = item.tolist()
        # 金叉
        if key_list[0] < 0 and key_list[1] > 0:
            return 1
        elif key_list[0] > 0 and key_list[1] < 0:
            return -1
        else:
            return 0  
        
# RSI回归
class RSIRegression(Factor):
    
    factor_code = 'rsi_regression'
    version = '1.0'
        
    _high_limit = 70
    _low_limit = 30
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data, create_signal = True):
        indicator = RSI(self._params)
        data = indicator.enrich(data)
        for param in self._params: 
            data[self.get_key(param)] = data[indicator.get_key(param)]
            if create_signal:
                data[self.get_signal(param)] = data[[self.get_key(param)]].rolling(2).apply(lambda item: self.get_action_mapping(param, item))
        return data 
    
    def get_action_mapping(self, param, item):
        key_list = item.tolist()
        if key_list[0] > self._low_limit and key_list[1] < self._low_limit:
            return 1
        elif key_list[0] < self._high_limit and key_list[1] > self._high_limit:
            return -1
        else:
            return 0 
            
    def obtain_visual_monitoring_parameters(self):
        return [factor.get_factor_code()]
    
# DRF回归
class DRFRegression(Factor):
    
    factor_code = 'drf_regression'
    version = '1.0'
    
    _high_limit = 0.7
    _low_limit = 0.3
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data, create_signal=True):
        indicator = DRF(self._params)
        data = indicator.enrich(data)
        for param in self._params: 
            data[self.get_key(param)] = data[indicator.get_key(param)]
            if create_signal:
                data[self.get_signal(param)] = data[[self.get_key(param)]].rolling(2).apply(lambda item: self.get_action_mapping(param, item))
        return data 

    def get_action_mapping(self, param, item):
        key_list = item.tolist()
        if key_list[0] > self._low_limit and key_list[1] < self._low_limit:
            return 1
        elif key_list[0] < self._high_limit and key_list[1] > self._high_limit:
            return -1
        else:
            return 0 
    
# KDJ回归
class KDJRegression(Factor):
    
    factor_code = 'kdj_regression'
    version = '1.0'
        
    _high_limit = 80
    _low_limit = 20
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data, create_signal=True):
        indicator = KDJ(self._params)
        data = indicator.enrich(data)
        for param in self._params: 
            data[self.get_key(param)] = data[indicator.get_d(param)]
            if create_signal:
                data[self.get_signal(param)] = data[[self.get_key(param)]].rolling(2).apply(lambda item: self.get_action_mapping(param, item))
        return data 
    
    def obtain_visual_monitoring_parameters(self):
        return [factor.get_factor_code()]

    def get_action_mapping(self, param, item):
        key_list = item.tolist()
        if key_list[0] > self._low_limit and key_list[1] < self._low_limit:
            return 1
        elif key_list[0] < self._high_limit and key_list[1] > self._high_limit:
            return -1
        else:
            return 0 
    
# WR回归
class WRRegression(Factor):
    
    factor_code = 'wr_regression'
    version = '1.0'
        
    _high_limit = 80
    _low_limit = 20
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data, create_signal=True):
        indicator = WR(self._params)
        data = indicator.enrich(data)
        for param in self._params: 
            data[self.get_key(param)] = data[indicator.get_key(param)]
            if create_signal:
                data[self.get_signal(param)] = data[[self.get_key(param)]].rolling(2).apply(lambda item: self.get_action_mapping(param, item))
        return data 

    def get_action_mapping(self, param, item):
        key_list = item.tolist()
        if key_list[0] > self._low_limit and key_list[1] < self._low_limit:
            return 1
        elif key_list[0] < self._high_limit and key_list[1] > self._high_limit:
            return -1
        else:
            return 0 
    
# UO突破
class UOPenetration(CombinedParamFactor):
    
    factor_code = 'uo_penetration'
    version = '1.0'
        
    _1st_high_limit = 65
    _2nd_high_limit = 70
    _2nd_low_limit = 35
    _1st_low_limit = 50
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data, create_signal=True):
        indicator = UO(self._params)
        data = indicator.enrich(data)
        data[self.get_key()] = data[indicator.get_key()]
        if create_signal:
            data[self.get_signal()] = data[[self.get_key()]].rolling(2).apply(lambda item: self.get_action_mapping(item))
        return data 

    def get_action_mapping(self, item):
        key_list = item.tolist()
        if key_list[0] < self._1st_high_limit and key_list[1] > self._1st_high_limit:
            return 1
        elif key_list[0] > self._2nd_high_limit and key_list[1] < self._2nd_high_limit:
            return -1
        elif key_list[0] < self._2nd_low_limit and key_list[1] > self._2nd_low_limit:
            return 1
        elif key_list[0] > self._1st_low_limit and key_list[1] < self._1st_low_limit:
            return -1
        else:
            return 0 
    
# RVI突破
class RVIPenetration(Factor):
    
    factor_code = 'rvi_penetration'
    version = '1.0'
        
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data, create_signal=True):
        indicator = RVI(self._params)
        data = indicator.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[indicator.get_key(param)] - data[indicator.get_rvis(param)]
            if create_signal:
                data[self.get_signal(param)] = data[[self.get_key(param)]].rolling(2).apply(lambda item: self.get_action_mapping(param, item))
        return data

    def get_action_mapping(self, param, item):
        key_list = item.tolist()
        if key_list[0] < 0 and key_list[1] > 0:
            return 1
        elif key_list[0] > 0 and key_list[1] < 0:
            return -1
        else:
            return 0 
    
# SO突破
class SOPenetration(Factor):
    
    factor_code = 'so_penetration'
    version = '1.0'
        
    _high_limit = 0.4
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data, create_signal=True):
        indicator = SO(self._params)
        data = indicator.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[indicator.get_key(param)]
            if create_signal:
                data[self.get_signal(param)] = data[[self.get_key(param)]].rolling(2).apply(lambda item: self.get_action_mapping(param, item))
        return data

    def get_action_mapping(self, param, item):
        key_list = item.tolist()
        if key_list[0] < self._high_limit and key_list[1] > self._high_limit:
            return 1
        elif key_list[0] > self._high_limit and key_list[1] < self._high_limit:
            return -1
        else:
            return 0 
     
if __name__ == '__main__':
    #图像分析
    # data = FileUtils.get_file_by_ts_code('688819.SH', is_reversion = True)
    # # factor = MACDPenetration([])
    # factor = MomentumPenetration([20])
    # factor = MomentumRegression([20])
    # factor = DiscreteIndex([10, 40])
    # factor = KDJRegression([9])
    # factor = DRFPenetration([0.3])
    # factor = WRRegression([30])
    # factor = UOPenetration([7, 14, 28])
    # factor = RVIPenetration([10])
    # data = factor.caculate(data)
    # factor = SOPenetration([10])
    # data = factor.caculate(data)
    # data['index_trade_date'] = pd.to_datetime(data['trade_date'])
    # data = data.set_index(['index_trade_date'])
    # draw_analysis_curve(data[(data['trade_date'] <= '20220125') & (data['trade_date'] > '20210101')], volume = False, show_signal = True, signal_keys = [factor.get_key()])
    print('aa')
    # print(factor.score(data))
    
    #模拟
    # data = FileUtils.get_file_by_ts_code('002531.SZ', is_reversion = False)
    # # factor = LowerHatch([5])
    # # factor = MeanInflectionPoint([20])
    # # factor = MeanPenetration([20])
    # # factor = EnvelopePenetration_MeanPercentage([20])
    # # factor = EnvelopePenetration_ATR([20])
    # # factor = MACDPenetration([])
    # # factor = KDJRegression([9])
    # # factor = DRFPenetration([0.3])
    # # factor = WRRegression([10])
    # # factor = UOPenetration([7, 14, 28])
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
    