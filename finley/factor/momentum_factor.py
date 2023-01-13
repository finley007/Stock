#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd

import os,sys 
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir) 
from persistence import FileUtils
from visualization import draw_analysis_curve
from indicator import MACD, MovingAverage, DIEnvelope, RSI, KDJ, DRF, WR, UO, RVI, SO
from simulator import simulate 
from factor.base_factor import Factor
import tools

# 动量突破
class MomentumPenetration(Factor):
    
    factor_code = 'momentum_penetration'
    version = '1.0'
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        data['momentum.' + str(self._params[0])] = data['close'] - data['close'].shift(self._params[0])
        #只取穿透点
        data[MomentumPenetration.factor_code] = 0
        data.loc[(data['momentum.' + str(self._params[0])].shift(1) < 0) & (data['momentum.' + str(self._params[0])] > 0), MomentumPenetration.factor_code] = data['momentum.' + str(self._params[0])] - data['momentum.' + str(self._params[0])].shift(1)
        data.loc[(data['momentum.' + str(self._params[0])].shift(1) > 0) & (data['momentum.' + str(self._params[0])] < 0), MomentumPenetration.factor_code] = data['momentum.' + str(self._params[0])] - data['momentum.' + str(self._params[0])].shift(1)
        return data 
    
# 动量回归
class MomentumRegression(Factor):
    
    factor_code = 'momentum_regression'
    version = '1.0'
        
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = MovingAverage([self._params[0]])
        data = indicator.enrich(data)
        data['momentum.' + str(self._params[0])] = (data['close'] - data['mean.'+str(self._params[0])])/data['mean.'+str(self._params[0])]
        #只取穿透点
        data[MomentumRegression.factor_code] = 0
        return data 

# 离散指标
class DiscreteIndex(Factor):
    
    factor_code = 'discrete_index'
    version = '1.0'
        
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = DIEnvelope([self._params])
        data = indicator.enrich(data)
        #只取穿透点
        data[DiscreteIndex.factor_code] = 0
        return data       

# MACD
class MACDPenetration(Factor):
    
    factor_code = 'macd_penetration'
    version = '1.0'
        
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = MACD(self._params)
        data = indicator.enrich(data)
        data['prev_gap'] = data['DIFF'].shift(1) - data['DEA'].shift(1)
        data['cur_gap'] = data['DIFF'] - data['DEA']
        data = data.dropna()
        data[MACDPenetration.factor_code] = 0
        #金叉
        data.loc[(data['prev_gap'] < 0) & (data['cur_gap'] > 0) & (data['DIFF'] > 0), MomentumRegression.factor_code] = abs(data['prev_gap']) + abs(data['cur_gap'])
        #死叉
        data.loc[(data['prev_gap'] > 0) & (data['cur_gap'] < 0) & (data['DIFF'] > 0), MomentumRegression.factor_code] = -(abs(data['prev_gap']) + abs(data['cur_gap']))
        return data  
    
   
# RSI突破
class RSIPenetration(Factor):
    
    factor_code = 'rsi_penetration'
    version = '1.0'
        
    _high_limit = 70
    _low_limit = 30
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = RSI([self._params[0]])
        data = indicator.enrich(data)
        #只取穿透点
        data[RSIPenetration.factor_code] = 0
        #突破下限买入
        data.loc[(data['rsi.' + str(self._params[0])].shift(1) > self._low_limit) & (data['rsi.' + str(self._params[0])] < self._low_limit), RSIPenetration.factor_code] = 1
        #突破上限卖出
        data.loc[(data['rsi.' + str(self._params[0])].shift(1) < self._high_limit) & (data['rsi.' + str(self._params[0])] > self._high_limit), RSIPenetration.factor_code] = -1
        return data 
    
    def obtain_visual_monitoring_parameters(self):
        return [factor.get_factor_code()]
    
# DRF突破
class DRFPenetration(Factor):
    
    factor_code = 'drf_penetration'
    version = '1.0'
    
    _high_limit = 0.7
    _low_limit = 0.3
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = DRF([self._params[0]])
        data = indicator.enrich(data)
        #只取穿透点
        data[DRFPenetration.factor_code] = 0
        #突破下限买入
        data.loc[(data['DRF.' + str(self._params[0])].shift(1) > self._low_limit) & (data['DRF.' + str(self._params[0])] < self._low_limit), DRFPenetration.factor_code] = 1
        #突破上限卖出
        data.loc[(data['DRF.' + str(self._params[0])].shift(1) < self._high_limit) & (data['DRF.' + str(self._params[0])] > self._high_limit), DRFPenetration.factor_code] = -1
        return data 
    
# KDJ回归
class KDJRegression(Factor):
    
    factor_code = 'kdj_regression'
    version = '1.0'
        
    _high_limit = 80
    _low_limit = 20
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = KDJ([self._params[0]])
        data = indicator.enrich(data)
        #只取穿透点
        data[KDJRegression.factor_code] = 0
        #突破下限买入
        data.loc[(data['D.' + str(self._params[0])].shift(1) > self._low_limit) & (data['D.' + str(self._params[0])] < self._low_limit), KDJRegression.factor_code] = 1
        #突破上限卖出
        data.loc[(data['D.' + str(self._params[0])].shift(1) < self._high_limit) & (data['D.' + str(self._params[0])] > self._high_limit), KDJRegression.factor_code] = -1
        return data 
    
    def obtain_visual_monitoring_parameters(self):
        return [factor.get_factor_code()]
    
# WR回归
class WRRegression(Factor):
    
    factor_code = 'wr_regression'
    version = '1.0'
        
    _high_limit = 80
    _low_limit = 20
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = WR([self._params[0]])
        data = indicator.enrich(data)
        #只取穿透点
        data[WRRegression.factor_code] = 0
        #突破下限买入
        data.loc[(data['WR.' + str(self._params[0])].shift(1) > self._low_limit) & (data['WR.' + str(self._params[0])] < self._low_limit), WRRegression.factor_code] = -100
        #突破上限卖出
        data.loc[(data['WR.' + str(self._params[0])].shift(1) < self._high_limit) & (data['WR.' + str(self._params[0])] > self._high_limit), WRRegression.factor_code] = 100
        return data 
    
# UO突破
class UOPenetration(Factor):
    
    factor_code = 'uo_penetration'
    version = '1.0'
        
    _high_limit = 65
    _high_limit1 = 70
    _low_limit = 35
    _low_limit1 = 50
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = UO(self._params)
        data = indicator.enrich(data)
        #只取穿透点
        data[UOPenetration.factor_code] = 0
        #向上突破65买入
        data.loc[(data['UO'].shift(1) < self._high_limit) & (data['UO'] > self._high_limit), UOPenetration.factor_code] = 100
        #向下回调70卖出
        data.loc[(data['UO'].shift(1) > self._high_limit1) & (data['UO'] < self._high_limit1), UOPenetration.factor_code] = -100
        #向下突破35并回调到35买入
        data.loc[(data['UO'].shift(1) < self._low_limit) & (data['UO'] > self._low_limit), UOPenetration.factor_code] = 100
        #向下突破50卖出
        data.loc[(data['UO'].shift(1) > self._low_limit1) & (data['UO'] < self._low_limit1), UOPenetration.factor_code] = -100
        return data 
    
# RVI突破
class RVIPenetration(Factor):
    
    factor_code = 'rvi_penetration'
    version = '1.0'
        
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = RVI(self._params)
        data = indicator.enrich(data)
        #只取穿透点
        data[RVIPenetration.factor_code] = 0
        #向上突破标记线买入
        data.loc[(data['RVI.'+str(self._params[0])].shift(1) < data['RVIS.'+str(self._params[0])].shift(1)) & (data['RVI.'+str(self._params[0])] > data['RVIS.'+str(self._params[0])]), RVIPenetration.factor_code] = 100
        #向下突破标记线卖出
        data.loc[(data['RVI.'+str(self._params[0])].shift(1) > data['RVIS.'+str(self._params[0])].shift(1)) & (data['RVI.'+str(self._params[0])] < data['RVIS.'+str(self._params[0])]), RVIPenetration.factor_code] = -100
        return data 
    
# SO突破
class SOPenetration(Factor):
    
    factor_code = 'so_penetration'
    version = '1.0'
        
    _high_limit = 0.4
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = SO(self._params)
        data = indicator.enrich(data)
        #只取穿透点
        data[SOPenetration.factor_code] = 0
        #向上突破标记线买入
        data.loc[(data['SO.'+str(self._params[0])].shift(1) < self._high_limit) & (data['SO.'+str(self._params[0])] > self._high_limit), SOPenetration.factor_code] = 100
        #向下突破标记线卖出
        data.loc[(data['SO.'+str(self._params[0])].shift(1) > self._high_limit) & (data['SO.'+str(self._params[0])] < self._high_limit), SOPenetration.factor_code] = -100
        return data 
     
if __name__ == '__main__':
    #图像分析
    data = FileUtils.get_file_by_ts_code('688819.SH', is_reversion = True)
    # # factor = MACDPenetration([])
    # # factor = MomentumPenetration([20])
    # factor = MomentumRegression([10])
    # # factor = DiscreteIndex([10, 40])
    # factor = KDJRegression([9])
    # factor = DRFPenetration([0.3])
    # factor = WRRegression([30])
    # factor = UOPenetration([7, 14, 28])
    # factor = RVIPenetration([10])
    # data = factor.caculate(data)
    factor = SOPenetration([10])
    data = factor.caculate(data)
    data['index_trade_date'] = pd.to_datetime(data['trade_date'])
    data = data.set_index(['index_trade_date'])
    draw_analysis_curve(data[(data['trade_date'] <= '20220125') & (data['trade_date'] > '20210101')], volume = False, show_signal = True, signal_keys = ['SO.10','so_penetration'])
    print('aa')
    print(factor.score(data))
    
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
    