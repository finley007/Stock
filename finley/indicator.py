#! /usr/bin/env python
# -*- coding:utf8 -*-

from abc import ABCMeta, abstractclassmethod
import pandas as pd
import numpy as np
import sys

import constants
from persistence import DaoMysqlImpl, FileUtils
from visualization import draw_analysis_curve

# 指标基类
class Indicator(metaclass = ABCMeta):
    
    key = ''
    _params = []
    
    def get_params(self):
        return self._params

    def get_key(self, param):
        return self.key + '.' + str(param)

    def get_keys(self):
        return list(map(lambda param: self.key + '.' + str(param), self._params))
    
    #静态数据部分
    @abstractclassmethod
    def enrich(self, data):
        pass
    

# 包络线基类  
class Envelope(Indicator):
    
    _percetage = 0.05
    _channel_width = 3
    
    @abstractclassmethod
    def get_high_value_key(self, param):
        pass
    
    @abstractclassmethod
    def get_low_value_key(self, param):
        pass
    
    # 默认用均线当中位线
    def get_middle_value_key(self, param):
        pass
    
    
# 移动平均线
class MovingAverage(Indicator):
    
    key = 'mean'
     
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        if self._params:
            for param in self._params:
                data[MovingAverage.key+"."+str(param)] = data["close"].rolling(param).mean()
        return data
    
# 指数移动平均线
class ExpMovingAverage(Indicator):
    
    key = 'exp_mean'
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        if self._params:
            for param in self._params:
                data.loc[:,ExpMovingAverage.key+"."+str(param)] = data["close"].ewm(alpha=2 / (param + 1), adjust=False).mean()
        return data.copy()
    
#一阶差分
class FirstDiff(Indicator):
    
    key = 'first_diff'
    
    def __init__(self, params, indicator):
        self._indicator = indicator
        self._params = params
        
    def enrich(self, data):
        data = self._indicator.enrich(data)
        data['normalized'] = data['close'].shift(self.get_params()[0]+1)*(1.1**(self.get_params()[0])-0.9**(self.get_params()[0]))/self.get_params()[0]
        data[self.get_key()] = (data[self._indicator.get_key() + '.' + str(self._indicator.get_params()[0])] - data[self._indicator.get_key() + '.' + str(self._indicator.get_params()[0])].shift(1))/data['normalized']
        data[self.get_key() + '.' + str(self.get_params()[0])] = data[FirstDiff.key + '.' + self._indicator.get_key()].ewm(alpha=2 / (self.get_params()[0] + 1), adjust=False).mean()
        return data.copy()
    
    def get_key(self):
        return FirstDiff.key + '.' + self._indicator.get_key()
            
# 自定义移动平均线
class CustomMovingAverage(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data, key):
        if self._params:
            for param in self._params:
                data.loc[:,"mean."+key+"."+str(param)] = data[key].rolling(param).mean()
        return data.copy()
    
# 自定义指数移动平均线
class CustomExpMovingAverage(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data, key):
        if self._params:
            for param in self._params:
                data.loc[:,"mean."+key+"."+str(param)] = data[key].ewm(alpha=2 / (param + 1), adjust=False).mean()
        return data.copy()

# ATR
class ATR(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        #当日振幅
        data['cur_day_amp'] = data['high'] - data['low']
        #昨日真实涨幅
        data['last_real_rise'] = abs(data['high'] - data['close'].shift(1))
        #昨日真实跌幅
        data['last_real_fall'] = abs(data['low'] - data['close'].shift(1))
        data['atr'] = data.apply(lambda x : max(x['cur_day_amp'], x['last_real_rise'], x['last_real_fall']), axis=1)
        return data
    
# ATR均值
class AtrMean(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        atr = ATR(self._params)
        data = atr.enrich(data)
        if self._params:
            for param in self._params:
                data["atr.mean."+str(param)] = data["atr"].rolling(param).mean()
        return data
    
# 标准差
class StandardDeviation(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        if self._params:
            for param in self._params:
                data["std."+str(param)] = data["close"].rolling(param).std()
        return data
    
# 动量
class Momentum(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        if self._params:
            for param in self._params:
                data["momentum."+str(param)] = data["close"] - data.shift(param)["close"]
        return data
    
# 动量
class DI(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        fast_indicator = MovingAverage([self._params[0]])
        data = fast_indicator.enrich(data)
        slow_indicator = MovingAverage([self._params[1]])
        data = slow_indicator.enrich(data)
        data['diff.' + str(self._params[0]) + '.' + str(self._params[1])] = data['mean.'+str(self._params[0])] - data['mean.'+str(self._params[1])]
        data['diff.' + str(self._params[1]) + '.stdev'] = (data['close'] - data['close'].shift(1)).rolling(self._params[1]).std()
        data['di.' + str(self._params[0]) + '.' + str(self._params[1])] = data['diff.' + str(self._params[0]) + '.' + str(self._params[1])]/data['diff.' + str(self._params[1]) + '.stdev']
        return data
    
# MACD
class MACD(Indicator):
    
    _fast_period = 12
    _slow_period = 26
    _dea_period = 9
    
    def __init__(self, params):
        self._params = params
        if (len(self._params) >= 3):
            self._fast_period = self._params[0]
            self._slow_period = self._params[1]
            self._dea_period = self._params[2]
        
    def enrich(self, data):
        data['fast.period'+str(self._fast_period)] = data['close'].ewm(alpha=2 / (self._fast_period + 1), adjust=False).mean()
        data['slow.period'+str(self._slow_period)] = data['close'].ewm(alpha=2 / (self._slow_period + 1), adjust=False).mean()
        
        data['DIFF'] = data['fast.period'+str(self._fast_period)] - data['slow.period'+str(self._slow_period)]
        data['DEA'] = data['DIFF'].ewm(alpha=2 / (self._dea_period + 1), adjust=False).mean()
        data['MACD'] = 2 * (data['DIFF'] - data['DEA'])
        return data

        
    
# 均值百分比包络
class MeanPercentageEnvelope(Envelope):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        moving_average = MovingAverage(self._params)
        data = moving_average.enrich(data)
        if self._params:
            for param in self._params:
                data[self.get_high_value_key(param)] = data["mean."+str(param)] * (1 + self._percetage * self._channel_width)
                data[self.get_low_value_key(param)] = data["mean."+str(param)] * (1 - self._percetage * self._channel_width)
        return data
    
    def get_high_value_key(self, param):
        return 'mean_percentage_envelope_high.'+str(param)
    
    def get_low_value_key(self, param):
        return 'mean_percentage_envelope_low.'+str(param)
    
# 价格百分比包络
class PricePercentageEnvelope(Envelope):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        moving_average = MovingAverage(self._params)
        data = moving_average.enrich(data)
        if self._params:
            for param in self._params:
                data[self.get_high_value_key(param)] = data["mean."+str(param)] + (data['close'] * self._percetage * self._channel_width)
                data[self.get_low_value_key(param)] = data["mean."+str(param)] - (data['close'] * self._percetage * self._channel_width)
        return data
    
    def get_high_value_key(self, param):
        return 'price_percentage_envelope_high.'+str(param)
    
    def get_low_value_key(self, param):
        return 'price_percentage_envelope_low.'+str(param)
    
# ATR均值包络
class ATREnvelope(Envelope):
    
    _percetage = 2
      
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data, atr_period = 14):
        moving_average = MovingAverage(self._params)
        data = moving_average.enrich(data)
        atr_mean = AtrMean([atr_period])
        data = atr_mean.enrich(data)
        if self._params:
            for param in self._params:
                data[self.get_high_value_key(param)] = data["mean."+str(param)] + (data["atr.mean."+str(atr_mean.get_params()[0])].shift(1) * self._percetage)
                data[self.get_low_value_key(param)] = data["mean."+str(param)] - (data["atr.mean."+str(atr_mean.get_params()[0])].shift(1) * self._percetage)
        return data
    
    def get_high_value_key(self, param):
        return 'atr_mean_envelope_high.'+str(param)
    
    def get_low_value_key(self, param):
        return 'atr_mean_envelope_low.'+str(param)
    
# 标准差包络
class StandardDeviationEnvelope(Envelope):
    
    _percetage = 1
      
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        moving_average = MovingAverage(self._params)
        data = moving_average.enrich(data)
        standard_deviation = StandardDeviation(self._params)
        data = standard_deviation.enrich(data)
        if self._params:
            for param in self._params:
                data[self.get_high_value_key(param)] = data["mean."+str(param)] + (data["std."+str(param)].shift(1) * self._percetage)
                data[self.get_low_value_key(param)] = data["mean."+str(param)] - (data["std."+str(param)].shift(1) * self._percetage)
        return data
    
    def get_high_value_key(self, param):
        return 'std_envelope_high.'+str(param)
    
    def get_low_value_key(self, param):
        return 'std_envelope_low.'+str(param)

# 肯特纳通道
class KeltnerEnvelope(Envelope):
    
    _percetage = 1
    _basic_price_key = 'basic_price'
    _volatility_key = 'volatility'
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data.loc[:, self._basic_price_key] = (data['close'] + data['low'] + data['high'])/3
        data.loc[:, self._volatility_key] = data['high'] - data['low']
        moving_average = CustomMovingAverage(self._params)
        data = moving_average.enrich(data, self._basic_price_key)
        data = moving_average.enrich(data, self._volatility_key)
        if self._params:
            for param in self._params:
                data.loc[:,self.get_middle_value_key(param)] = data["mean."+self._basic_price_key+"."+str(param)]
                data.loc[:,self.get_high_value_key(param)] = data[self.get_middle_value_key(param)] + data["mean."+self._volatility_key+"."+str(param)]
                data.loc[:,self.get_low_value_key(param)] = data[self.get_middle_value_key(param)] - data["mean."+self._volatility_key+"."+str(param)]
        return data.copy()
    
    def get_middle_value_key(self, param):
        return "mean." + self._basic_price_key + "." + str(param)
    
    def get_high_value_key(self, param):
        return 'keltner_envelope_high.'+str(param)
    
    def get_low_value_key(self, param):
        return 'keltner_envelope_low.'+str(param)
    
# 金肯特纳通道
class AdvanceKeltnerEnvelope(Envelope):
    
    _percetage = 1
    _basic_price_key = 'basic_price'
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data.loc[:, self._basic_price_key] = (data['close'] + data['low'] + data['high'])/3
        moving_average = CustomExpMovingAverage(self._params)
        data = moving_average.enrich(data, self._basic_price_key)
        atr_mean = AtrMean([10])
        data = atr_mean.enrich(data)
        if self._params:
            for param in self._params:
                data.loc[:,self.get_middle_value_key(param)] = data["mean."+self._basic_price_key+"."+str(param)]
                data.loc[:,self.get_high_value_key(param)] = data[self.get_middle_value_key(param)] + data["atr.mean.10"]
                data.loc[:,self.get_low_value_key(param)] = data[self.get_middle_value_key(param)] - data["atr.mean.10"]
        return data.copy()
    
    def get_middle_value_key(self, param):
        return "mean." + self._basic_price_key + "." + str(param)
    
    def get_high_value_key(self, param):
        return 'keltner_envelope_high.'+str(param)
    
    def get_low_value_key(self, param):
        return 'keltner_envelope_low.'+str(param)
    

    @abstractclassmethod
    def get_low_value_key(self, param):
        pass
    
    # 默认用均线当中位线
    def get_middle_value_key(self, param):
        return 'mean.' + str(param)
    
    
# 移动平均线
class MovingAverage(Indicator):
    
    key = 'moving_average'
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        if self._params:
            for param in self._params:
                data[self.get_key(param)] = data["close"].rolling(param).mean()
        return data
    
# 自定义移动平均线
class CustomMovingAverage(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data, key):
        if self._params:
            for param in self._params:
                data.loc[:,"mean."+key+"."+str(param)] = data[key].rolling(param).mean()
        return data.copy()
    
# 自定义指数移动平均线
class CustomExpMovingAverage(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data, key):
        if self._params:
            for param in self._params:
                data.loc[:,"mean."+key+"."+str(param)] = data[key].ewm(alpha=2 / (param + 1), adjust=False).mean()
        return data.copy()

# ATR
class ATR(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        #当日振幅
        data['cur_day_amp'] = data['high'] - data['low']
        #昨日真实涨幅
        data['last_real_rise'] = abs(data['high'] - data['close'].shift(1))
        #昨日真实跌幅
        data['last_real_fall'] = abs(data['low'] - data['close'].shift(1))
        data['atr'] = data.apply(lambda x : max(x['cur_day_amp'], x['last_real_rise'], x['last_real_fall']), axis=1)
        return data
    
# ATR均值
class AtrMean(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        atr = ATR(self._params)
        data = atr.enrich(data)
        if self._params:
            for param in self._params:
                data["atr.mean."+str(param)] = data["atr"].rolling(param).mean()
        return data
    
# 标准差
class StandardDeviation(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        if self._params:
            for param in self._params:
                data["std."+str(param)] = data["close"].rolling(param).std()
        return data
    
# 动量
class Momentum(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        if self._params:
            for param in self._params:
                data["momentum."+str(param)] = data["close"] - data.shift(param)["close"]
        return data
    
# 离散指标
class DI(Indicator):
    """
    （快周期均值-慢周期均值）/ 基于慢周期的价格增量标准差
    """
    
    key = 'di'
    
    def __init__(self, params):
        # 只包含两个值，一个快线值，一个慢线值
        self._params = params
        
    def get_key(self):
        return self.key + '.' + str(self._params[0]) + '.' + str(self._params[1])
    
    def enrich(self, data):
        fast_indicator = MovingAverage([self._params[0]])
        data = fast_indicator.enrich(data)
        slow_indicator = MovingAverage([self._params[1]])
        data = slow_indicator.enrich(data)
        data['diff.' + str(self._params[0]) + '.' + str(self._params[1])] = data[fast_indicator.get_key(self._params[0])] - data[slow_indicator.get_key(self._params[1])]
        data['diff.' + str(self._params[1]) + '.stdev'] = (data['close'] - data['close'].shift(1)).rolling(self._params[1]).std()
        data[self.get_key()] = data['diff.' + str(self._params[0]) + '.' + str(self._params[1])]/data['diff.' + str(self._params[1]) + '.stdev']
        return data

# MACD
class MACD(Indicator):
    
    key = 'macd'
    
    _fast_period = 12
    _slow_period = 26
    _dea_period = 9
    
    def __init__(self, params=[]):
        if (len(params) >= 3):
            self._fast_period = params[0]
            self._slow_period = params[1]
            self._dea_period = params[2]
            
    def get_key(self):
        return self.key + '.' + str(self._fast_period) + '.' + str(self._slow_period) + '.' + str(self._dea_period)
        
    def enrich(self, data):
        #12日平滑移动平均值
        data['fast.period'+str(self._fast_period)] = data['close'].ewm(alpha=2 / (self._fast_period + 1), adjust=False).mean()
        #26日平滑移动平均值
        data['slow.period'+str(self._slow_period)] = data['close'].ewm(alpha=2 / (self._slow_period + 1), adjust=False).mean()
        data['DIFF'] = data['fast.period'+str(self._fast_period)] - data['slow.period'+str(self._slow_period)]
        #对DIFF做9日平滑移动平均值
        data['DEA'] = data['DIFF'].ewm(alpha=2 / (self._dea_period + 1), adjust=False).mean()
        data[self.get_key()] = 2 * (data['DIFF'] - data['DEA'])
        return data

# 离散指标包络
class DIEnvelope(Envelope):
    
    key = 'di_envelope'
    
    def __init__(self, params):
        self._params = params
        self._indicator = DI(self._params)
        
    def enrich(self, data):
        data = self._indicator.enrich(data)
        data[self.get_high_value_key()] = data[self._indicator.get_key()].rolling(self._params[1]).std()
        data[self.get_low_value_key()] = -data[self._indicator.get_key()].rolling(self._params[1]).std()
        return data
    
    def get_high_value_key(self):
        return self.key + '.high.' + str(self._params[0]) + '.' + str(self._params[1])
    
    def get_low_value_key(self):
        return self.key + '.low.' + str(self._params[0]) + '.' + str(self._params[1])
    
    def get_middle_value_key(self):
        return self._indicator.get_key()
    
# RSI
class RSI(Indicator):

    key = 'rsi'
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['increase'] = 0
        data['decrease'] = 0
        data['change'] = data['close'] - data['close'].shift(1)
        data.loc[data['change'] > 0, 'increase'] = data['change']
        data.loc[data['change'] < 0, 'decrease'] = -data['change']
        for param in self._params:
            data['au'+str(param)] = data['increase'].rolling(param).sum()
            data['ad'+str(param)] = data['decrease'].rolling(param).sum()
            data[self.get_key(param)] = 100 - (100 / (1 + data['au'+str(param)]/data['ad'+str(param)]))
        return data
    
# RSV
class RSV(Indicator):
    
    key = 'rsv'

    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        for param in self._params:
            data[self.get_key(param)] = 100 * (data['close'] - data['close'].rolling(param).min())/(data['close'].rolling(param).max() - data['close'].rolling(param).min())
        return data
    
# KDJ
class KDJ(Indicator):
    
    key = 'kdj'

    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        rsv = RSV(self._params)
        data = rsv.enrich(data)
        for param in self._params:
            data[self.get_k(param)] = data[rsv.get_key(param)].ewm(alpha=1/3, adjust=False).mean()
            data[self.get_d(param)] = data[self.get_k(param)].ewm(alpha=1/3, adjust=False).mean()
            data[self.get_j(param)] = 3*data[self.get_d(param)] + 2*data[self.get_k(param)]
        return data

    def get_k(self, param):
        return 'K.'+str(param)

    def get_d(self, param):
        return 'D.'+str(param)

    def get_j(self, param):
        return 'J.'+str(param)
    
#DRF
class DRF(Indicator):
    
    key = 'drf'

    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['BP'] = data['high'] - data['open']
        data['SP'] = data['close'] - data['low']
        data['DRF'] = (data['BP'] + data['SP'])/(2*(data['high'] - data['low'])) 
        for param in self._params:
            data[self.get_key(param)] = data['DRF'].ewm(alpha=param, adjust = False).mean()
        return data
    
#WR
class WR(Indicator):

    key = 'wr'
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        for param in self._params:
            data['high.'+str(param)] = data['high'].rolling(param).max()
            data['low.'+str(param)] = data['low'].rolling(param).min()
            data[self.get_key(param)] = 100*(data['high.'+str(param)] - data['close'])/(data['high.'+str(param)] - data['low.'+str(param)]) 
        return data
    
#UO   
class UO(Indicator):
    """
    终极波动指标
    https://baike.baidu.com/item/%E7%BB%88%E6%9E%81%E6%B3%A2%E5%8A%A8%E6%8C%87%E6%A0%87/1982936
    """
    
    key = 'uo'

    _1st_period = 7
    _2nd_period = 14
    _3rd_period = 28

    def __init__(self, params):
        self._params = params

    def __init__(self, params=[]):
        if (len(params) >= 3):
            self._1st_period = params[0]
            self._2nd_period = params[1]
            self._3rd_period = params[2]
            
    def get_key(self):
        return self.key + '.' + str(self._1st_period) + '.' + str(self._2nd_period) + '.' + str(self._3rd_period)
        
    def enrich(self, data):
        data['last_close'] = data['close'].shift(1)
        data['TL'] = data.apply(lambda x : min(x['last_close'], x['low']), axis=1)
        data['BP'] = data['close'] - data['TL']
        data['TR'] = data.apply(lambda x : max(x['last_close'], x['high']), axis=1) - data['TL']
        data['avg.'+str(self._1st_period)] = data['BP'].rolling(self._1st_period).sum()/data['TR'].rolling(self._1st_period).sum()
        data['avg.'+str(self._2nd_period)] = data['BP'].rolling(self._2nd_period).sum()/data['TR'].rolling(self._2nd_period).sum()
        data['avg.'+str(self._3rd_period)] = data['BP'].rolling(self._3rd_period).sum()/data['TR'].rolling(self._3rd_period).sum()
        data[self.get_key()] = 100*(4*data['avg.'+str(self._1st_period)]+2*data['avg.'+str(self._2nd_period)]+data['avg.'+str(self._3rd_period)])/7
        return data
    
#RVI
class RVI(Indicator):
    
    key = 'rvi'

    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['CO'] = data['close'] - data['open']
        data['HL'] = data['high'] - data['low']
        data['V1'] = (data['CO'] + 2*data['CO'].shift(1) + 2*data['CO'].shift(2)+ data['CO'].shift(3))/6
        data['V2'] = (data['HL'] + 2*data['HL'].shift(1) + 2*data['HL'].shift(2)+ data['HL'].shift(3))/6
        for param in self._params:
            data['S1'] = data['V1'].rolling(param).sum()
            data['S2'] = data['V2'].rolling(param).sum()
            data[self.get_key(param)] = data['S1']/data['S2']
            data[self.get_rvis(param)] = (data[self.get_key(param)] + 2*data[self.get_key(param)].shift(1) + 2*data[self.get_key(param)].shift(2)+ data[self.get_key(param)].shift(3))/6
        return data 

    def get_rvis(self, param):
        return 'rvis.'+str(param)
    
#TSI
class TSI(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['delta'] = data['close'] - data['close'].shift(1)
        data['delta_abs'] = data.apply(lambda x : abs(x['delta']), axis=1)
        data['delta_mean'] = data['delta'].ewm(span=self._params[0]).mean()
        data['delta_abs_mean'] = data['delta_abs'].ewm(span=self._params[0]).mean()
        data['TSI.'+str(self._params[0])+'.'+str(self._params[1])] = 100*data['delta_mean'].ewm(span=self._params[1]).mean()/data['delta_abs_mean'].ewm(span=self._params[1]).mean()
        return data   
    
#SO Strength Oscillator
class SO(Indicator):
    
    key = 'so'

    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['delta'] = (data['close'] - data['close'].shift(1))
        data['amp'] = (data['high'] - data['low'])
        for param in self._params:
            data['delta_mean.' + str(param)] = data['delta'].rolling(param).mean()
            data['amp_mean.' + str(param)] = data['amp'].rolling(param).mean()
            data[self.get_key(param)] = data['delta_mean.' + str(param)]/data['amp_mean.' + str(param)]
        return data 
    
#ADX
class ADX(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['delta_high'] = data['high'] - data['high'].shift(1)
        data['delta_low'] = data['low'].shift(1) - data['low']
        data.loc[((data['delta_high'] < 0) & (data['delta_low'] < 0)) | (data['delta_high'] == data['delta_low']), 'DM+'] = 0
        data.loc[((data['delta_high'] < 0) & (data['delta_low'] < 0)) | (data['delta_high'] == data['delta_low']), 'DM-'] = 0
        data.loc[(data['delta_high'] > data['delta_low']), 'DM+'] = data['delta_high']
        data.loc[(data['delta_high'] > data['delta_low']), 'DM-'] = 0
        data.loc[(data['delta_high'] < data['delta_low']), 'DM+'] = 0
        data.loc[(data['delta_high'] < data['delta_low']), 'DM-'] = data['delta_low']
        indicator = ATR(self._params)
        indicator.enrich(data)
        data['DM+.'+str(self._params[0])] = data['DM+'].ewm(span=self._params[0], adjust=False).mean()
        data['DM-.'+str(self._params[0])] = data['DM-'].ewm(span=self._params[0], adjust=False).mean()
        data['atr.'+str(self._params[0])] = data['atr'].ewm(span=self._params[0], adjust=False).mean()
        data['DI+.'+str(self._params[0])] = data['DM+.'+str(self._params[0])]/data['atr.'+str(self._params[0])]
        data['DI-.'+str(self._params[0])] = data['DM-.'+str(self._params[0])]/data['atr.'+str(self._params[0])]
        data['DI.SUM.'+str(self._params[0])] = data['DI+.'+str(self._params[0])] + data['DI-.'+str(self._params[0])]
        data['DI.SUB.'+str(self._params[0])] = abs(data['DI+.'+str(self._params[0])] - data['DI-.'+str(self._params[0])])
        data['DX.'+str(self._params[0])] = (data['DI.SUB.'+str(self._params[0])] * 100)/data['DI.SUM.'+str(self._params[0])]
        # data['ADX.'+str(self._params[0])] = data['DX.'+str(self._params[0])].ewm(span=self._params[0], adjust=False).mean()
        data['ADX.'+str(self._params[0])] = data['DX.'+str(self._params[0])].mean()
        return data 
    
    
#强力指标
class FI(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['fi'] = (data['close'] - data['close'].shift(1)) * data['vol']
        data['fi.'+str(self._params[0])] = data['fi'].ewm(span=self._params[0], adjust=False).mean()
        return data 
    
#能量潮指标
class OBV(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['last_close'] = data['close'].shift(1)
        data['sign'] = data.apply(lambda x : self.get_trend_indicator(x), axis=1)
        data['signed_vol'] = data['sign'] * data['vol']
        signed_vol = np.array(data['signed_vol'])
        obv = np.cumsum(signed_vol)
        data['obv'] = obv
        return data
        
    def get_trend_indicator(self, x):
        if (x['close'] > x['last_close']): 
            return 1
        if (x['close'] < x['last_close']):
            return -1
        return 0
    
#资金流量指标
class MFI(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['tp'] = (data['high'] + data['low'] + data['close'])/3
        data['last_tp'] = data['tp'].shift(1)
        data['sign'] = data.apply(lambda x : self.get_trend_indicator(x), axis=1)
        data['plus_signed_tp'] = 0
        data['minus_signed_tp'] = 0
        data.loc[(data['sign'] == 1), 'plus_signed_tp'] = data['tp'] * data['vol']
        data.loc[(data['sign'] == -1), 'minus_signed_tp'] = data['tp'] * data['vol']
        data['pmf.'+str(self._params[0])] = data['plus_signed_tp'].rolling(self._params[0]).sum()
        data['nmf.'+str(self._params[0])] = data['minus_signed_tp'].rolling(self._params[0]).sum()
        data['mfi.'+str(self._params[0])] = 100 - (100 / (1 + data['pmf.'+str(self._params[0])]/data['nmf.'+str(self._params[0])]))
        return data
        
    def get_trend_indicator(self, x):
        if (x['tp'] > x['last_tp']): 
            return 1
        if (x['tp'] < x['last_tp']):
            return -1
        return 0
    
#价量趋势指标
class PVT(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['last_close'] = data['close'].shift(1)
        data['factor'] = (data['close'] - data['last_close'])/data['last_close']
        data['factor_vol'] = data['factor'] * data['vol']
        factor_vol = np.array(data['factor_vol'])[1:]
        pvt = np.cumsum(factor_vol)
        pvt = np.insert(pvt, 0, 0)
        data['pvt'] = pvt
        return data
        
  
if __name__ == '__main__':
    # data = FileUtils.get_file_by_ts_code('600438.SH', True)
    # data = FileUtils.get_file_by_product_and_instrument('C', 'C1105')
    # data = data.iloc[::-1]
    # indicator = MovingAverage([20])
    # data = indicator.enrich(data)
    # indicator1 = ExpMovingAverage([20])
    # data = indicator.enrich(data)
    # indicator = StandardDeviation([5])
    # data = indicator.enrich(data)
    # indicator = AtrMean([14])
    # data = indicator.enrich(data)
    # indicator = MeanPercentageEnvelope([5])
    # data = indicator.enrich(data)
    # indicator = PricePercentageEnvelope([5])
    # data = indicator.enrich(data)
    # indicator = ATREnvelope([5])
    # data = indicator.enrich(data)
    # indicator = Momentum([5, 20])
    # data = indicator.enrich(data)
    # indicator = StandardDeviationEnvelope([5])
    # data = indicator.enrich(data)
    # indicator = MACD([])
    # data = indicator.enrich(data)
    # indicator = KeltnerEnvelope([20])
    # data = indicator.enrich(data)
    # indicator = AdvanceKeltnerEnvelope([20])
    # data = indicator.enrich(data)
    # indicator = DIEnvelope([10,40])
    # data = indicator.enrich(data)
    # indicator = RSI([14])
    # data = indicator.enrich(data)
    # indicator = KDJ([9])
    # data = indicator.enrich(data)
    # indicator = DRF([0.3])
    # data = indicator.enrich(data)
    # indicator = WR([30])
    # data = indicator.enrich(data)
    # indicator = UO([7,14,28])
    # data = indicator.enrich(data)
    # indicator = TSI([25,13])
    # data = indicator.enrich(data)
    # indicator = SO([10])
    # data = indicator.enrich(data)
    # indicator = ADX([14])
    # data = indicator.enrich(data)
    # indicator = FI([26])
    # data = indicator.enrich(data)
    # indicator = OBV([])
    # data = indicator.enrich(data)
    # indicator = MFI([14])
    # data = indicator.enrich(data)
    # indicator = PVT([])
    # data = indicator.enrich(data)
    # indicator = FirstDiff([13], indicator1)
    # data = indicator.enrich(data)
    # data['index_trade_date'] = pd.to_datetime(data['trade_date'])
    # data = data.set_index(['index_trade_date'])
    # data['volume'] = data['vol']
    # draw_analysis_curve(data[data['trade_date'] > '20220101'], volume = True, show_signal = True, signal_keys = ['mean.20','first_diff.mean.13'])
    # draw_analysis_curve(data, volume = True, show_signal = False, signal_keys = ['mean.5'])
    print("aa")