#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd

import os,sys
import numpy as np

parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir)  
from visualization import draw_analysis_curve
from persistence import DaoMysqlImpl, FileUtils
from factor.base_factor import Factor, CombinedParamFactor
from visualization import draw_histogram
from indicator import RSI
from simulator import SignalClosingStragegy, SimulationConfig, StockSimulator, FixTimeClosingStragegy

'''
过去n天上涨比例因子
''' 
class RisingTrend(Factor):
    
    factor_code = 'rising_trend'
    
    def __init__(self, params):
        self._params = params
        
    def score(self, data):
        #只计算最后一天的因子，为了减小计算量取计算最后一天因子值所需的最小数据集
        data = data.loc[len(data)-self._params[0]:len(data)-1]
        data = self.caculate(data)
        #最近一天的最小参数
        return data.iloc[len(data) - 1][self._factor_code + str(self._params[0])]
        
    def caculate(self, data, create_signal=True):
        for param in self._params:
            data["change"] = data["close"] - data.shift(1)['close']
            data[self.get_key(param)] = data.rolling(window = param)["change"].apply(lambda x: self.get_rising_rate(x), raw=False)
            if create_signal:
                data[self.get_signal(param)] = data.apply(lambda item: self.get_action_mapping(param, item), axis = 1)
        return data
    
    def get_rising_rate(self, x):
        return len(x[x > 0]) / len(x)
    
    def get_action_mapping(self, param, item):
        threshold = {
            5 : 0.8,
            10 : 0.7
        }
        if (item[self.get_key(param)] >= threshold[param]):
            return 1
        else:
            return 0
 
'''
过去n天下跌比例因子
'''    
class FallingTrend(Factor):
    
    factor_code = 'fallings_trend'
    
    def __init__(self, params):
        self._params = params
    
    def score(self, data):
        data = data.loc[len(data)-self._params[0]:len(data)-1]
        data = self.caculate(data)
        #最近一天的最小参数
        return data.iloc[len(data) - 1][self._factor_code + str(self._params[0])]
    
    def caculate(self, data, create_signal=True):
        for param in self._params:
            data["change"] = data["close"] - data.shift(1)['close']
            data[self.get_key(param)] = data.rolling(window = param)["change"].apply(lambda x: self.get_falling_rate(x), raw=False);
            if create_signal:
                data[self.get_signal(param)] = data.apply(lambda item: self.get_action_mapping(param, item), axis = 1)
        return data
    
    def get_falling_rate(self, x):
        return len(x[x < 0]) / len(x)
    
    def get_action_mapping(self, param, item):
        threshold = {
            10 : 0.9,
            15 : 0.8,
            20 : 0.7
        }
        if (item[self.get_key(param)] >= threshold[param]):
            return 1
        else:
            return 0
        
'''
带下长影线的局部低点
'''   
class LowerHatch(Factor):
    
    factor_code = 'lower_hatch'
    
    def __init__(self, params):
        self._params = params
    
    def score(self, data):
        data = data.loc[len(data)-self._params[0]:len(data)-1]
        data = self.caculate(data)
        #最近一天的最小参数
        return data.iloc[len(data) - 1][self._factor_code]
    
    def caculate(self, data, create_signal = True):
        # 计算下影线比例
        data.loc[data["open"] >= data["close"], 'len_lower_hatch'] = data["close"] - data["low"]
        data.loc[data["open"] < data["close"], 'len_lower_hatch'] = data["open"] - data["low"]
        data['amplitude'] = data["high"] - data["low"]
        data["lower_hatch_scale"] = data['len_lower_hatch'] / data["amplitude"]
        #下降幅度，参数范围内最高值到当日收盘价的降幅
        for param in self._params:
            data['decline.' + str(param)] = ((data['high'].rolling(window = param).max() - data['close'])/data['close']) * pow(0.9, param)/(1 - pow(0.9, param))
            data[self.get_key(param)] = (data["lower_hatch_scale"] + 3 * data['decline.' + str(param)])/4
            if create_signal:
                data[self.get_signal(param)] = data.apply(lambda item: self.get_action_mapping(param, item), axis = 1)
        return data
    
    def get_action_mapping(self, param, item):
        threshold = {
            10 : 0.7
        }
        if (item[self.get_key(param)] >= threshold[param]):
            return 1
        else:
            return 0
        
'''
RSI 金叉
'''
class RSIGoldenCross(CombinedParamFactor):

    factor_code = 'rsi_golden_cross'
    version = '1.0'
        
    def __init__(self, params):
        self._params = params

    def caculate(self, data, create_signal = True):
        indicator = RSI(self._params)
        data = indicator.enrich(data)
        data[self.get_key()] = data[indicator.get_key(self._params[0])] - data[indicator.get_key(self._params[1])]
        if create_signal:
            data[self.get_signal()] = data[[self.get_key()]].rolling(2).apply(lambda item: self.get_action_mapping(item))
        return data
    
    def get_action_mapping(self, item):
        key_list = item.tolist()
        if key_list[0] < 0 and key_list[1] > 1:
            return 1
        elif key_list[0] > 0 and key_list[1] < 1:
            return -1
        else:
            return 0 
        
        
if __name__ == '__main__':
    # 因子分析
    # factor = RisingTrend([5, 10])
    # factor = FallingTrend([10, 15, 20])
    # factor = LowerHatch([10])
    # print(factor.analyze(stock_list=['601318.SH']))
    
    # 收益分析
    # data = FileUtils.get_file_by_ts_code('000610.SZ', is_reversion = True)
    # data = factor.caculate(data)
    # data = Factor.caculate_ret(data, [1, 2, 3, 4, 5])
    # data = data.dropna()
    # ret_list = data[data[factor.get_signal(10)] == 1]['ret.1'].tolist()
    # ret = np.array(ret_list)
    # result = {
    #         'max' : np.amax(ret),
    #         'min' : np.amin(ret),
    #         'range' : np.ptp(ret),
    #         'mean' : np.mean(ret),
    #         'median' : np.median(ret),
    #         'std' : np.std(ret),
    #         'var' : np.var(ret)
    # }
    # print(result)
    # draw_histogram(ret_list, bin_num=100)
    
    #图像分析
    # data = FileUtils.get_file_by_ts_code('002454.SZ', is_reversion = True)
    # factor = RSIGoldenCross([7,14])
    # data = factor.caculate(data)
    # data['index_trade_date'] = pd.to_datetime(data['trade_date'])
    # data = data.set_index(['index_trade_date'])
    # draw_analysis_curve(data[(data['trade_date'] <= '20230324') & (data['trade_date'] > '20230101')], volume = False, show_signal = True, signal_keys = [factor.get_key(), factor.get_signal()])
    # print('aa')
    
    #模拟
    data = FileUtils.get_file_by_ts_code('002454.SZ', is_reversion = False)
    # # factor = LowerHatch([5])
    # factor = MeanInflectionPoint([20])
    # # # factor = MeanPenetration([20])
    # # # factor = EnvelopePenetration_MeanPercentage([20])
    # # # factor = EnvelopePenetration_ATR([20])
    factor = RSIGoldenCross([7,14])
    simulator = StockSimulator()
    config = SimulationConfig()
    config.set_closing_stratege(SignalClosingStragegy())
    # config.set_closing_stratege(FixTimeClosingStragegy(3))
    simulator.simulate(factor, data, start_date = '20230101', save = False, config = config)
    
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