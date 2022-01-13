#! /usr/bin/env python
# -*- coding:utf8 -*-

from abc import ABCMeta, abstractclassmethod
import pandas as pd
import sys

import constants
from persistence import DaoMysqlImpl, FileUtils
from visualization import draw_analysis_curve

# 指标基类
class Indicator(metaclass = ABCMeta):
    
    _params = []
    
    #静态数据部分
    @abstractclassmethod
    def enrich(self, data):
        pass
    
    def get_params(self):
        return self._params;

# 包络线基类  
class Envelope(Indicator):
    
    _percetage = 0.05
    _channel_width = 1
    
    @abstractclassmethod
    def get_high_value_key(self, param):
        pass
    
    @abstractclassmethod
    def get_low_value_key(self, param):
        pass
    
    # 默认用均线当中位线
    def get_middle_value_key(self, param):
        return 'mean.' + str(param)
    
    
# 移动平均线
class MovingAverage(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        if self._params:
            for param in self._params:
                data["mean."+str(param)] = data["close"].rolling(param).mean()
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
    
    _percetage = 1
      
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
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        if self._params:
            for param in self._params:
                data["mean."+str(param)] = data["close"].rolling(param).mean()
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

# 离散指标包络
class DIEnvelope(Envelope):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        di_indicator = DI(self._params)
        data = di_indicator.enrich(data)
        data[self.get_high_value_key(str(self._params[0])+'.'+str(self._params[1]))] = data["di."+str(di_indicator.get_params()[0])+'.'+str(di_indicator.get_params()[1])].rolling(di_indicator.get_params()[1]).std()
        data[self.get_low_value_key(str(self._params[0])+'.'+str(self._params[1]))] = -data["di."+str(di_indicator.get_params()[0])+'.'+str(di_indicator.get_params()[1])].rolling(di_indicator.get_params()[1]).std()
        return data
    
    def get_high_value_key(self, param):
        return 'di_band_envelope_high.'+str(param)
    
    def get_low_value_key(self, param):
        return 'di_band_envelope_low.'+str(param)
    
# RSI
class RSI(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['increase'] = 0
        data['decrease'] = 0
        data['change'] = data['close'] - data['close'].shift(1)
        data.loc[data['change'] > 0, 'increase'] = data['change']
        data.loc[data['change'] < 0, 'decrease'] = -data['change']
        data['au'] = data['increase'].rolling(self._params[0]).sum()
        data['ad'] = data['decrease'].rolling(self._params[0]).sum()
        data['rsi.' + str(self._params[0])] = 100 - (100 / (1 + data['au']/data['ad']))
        return data
    
# RSV
class RSV(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['rsv.'+str(self._params[0])] = 100 * (data['close'] - data['close'].rolling(self._params[0]).min())/(data['close'].rolling(self._params[0]).max() - data['close'].rolling(self._params[0]).min())
        return data
    
# KDJ
class KDJ(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        rsv = RSV(self._params)
        data = rsv.enrich(data)
        data['K.'+str(self._params[0])] = data['rsv.'+str(self._params[0])].ewm(alpha=1/3, adjust=False).mean()
        data['D.'+str(self._params[0])] = data['K.'+str(self._params[0])].ewm(alpha=1/3, adjust=False).mean()
        data['J.'+str(self._params[0])] = 3*data['D.'+str(self._params[0])] + 2*data['K.'+str(self._params[0])]
        return data
    
#DRF
class DRF(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['BP'] = data['high'] - data['open']
        data['SP'] = data['close'] - data['low']
        data['DRF'] = (data['BP'] + data['SP'])/(2*(data['high'] - data['low'])) 
        data['DRF.' + str(self._params[0])] = data['DRF'].ewm(alpha=self._params[0], adjust = False).mean()
        return data
    
#WR
class WR(Indicator):
    
    def __init__(self, params):
        self._params = params
        
    def enrich(self, data):
        data['high.'+str(self._params[0])] = data['high'].rolling(self._params[0]).max()
        data['low.'+str(self._params[0])] = data['low'].rolling(self._params[0]).min()
        data['WR'] = 100*(data['high.'+str(self._params[0])] - data['close'])/(data['high.'+str(self._params[0])] - data['low.'+str(self._params[0])]) 
        return data
  
if __name__ == '__main__':
    data = FileUtils.get_file_by_ts_code('000651.SZ', True)
    # data = data.iloc[::-1]
    # indicator = MovingAverage([20])
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
    indicator = WR([30])
    data = indicator.enrich(data)
    data['index_trade_date'] = pd.to_datetime(data['trade_date'])
    data = data.set_index(['index_trade_date'])
    draw_analysis_curve(data[data['trade_date'] > '20210917'], volume = False, show_signal = True, signal_keys = ['WR'])
    print("aa")