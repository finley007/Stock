#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd

import os,sys 
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir) 
from persistence import FileUtils
from visualization import draw_analysis_curve
from indicator import MovingAverage, ATREnvelope, MeanPercentageEnvelope, KeltnerEnvelope, AdvanceKeltnerEnvelope
from simulator import StockSimulator, FutrueSimulator, capital_curve_simulate
from factor.base_factor import Factor
from financetools import create_k_line

# 移动平均线拐点 
class MeanInflectionPoint(Factor):
    
    signal_delay = 2
    factor_code = 'mean_inflection_point'
    version = '2.5'
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = MovingAverage(self._params)
        data = indicator.enrich(data)
        indicator_key = 'mean.' + str(self._params[0])
        # 后一日的增量
        data['next_trend'] = data[indicator_key].shift(-1) - data[indicator_key]
        # data['next_trend'] = (data['close'] - data['close'].shift(self._params[0]))/5
        # 前一日的增量
        data['prev_trend'] = data[indicator_key] - data[indicator_key].shift(1)
        #只取拐点
        data[MeanInflectionPoint.factor_code] = 0
        #计算相对变化
        data.loc[(data['next_trend'] > 0) & (data['prev_trend'] < 0), MeanInflectionPoint.factor_code] = (abs(data['next_trend']) + abs(data['prev_trend']))/data[indicator_key]
        data.loc[(data['next_trend'] < 0) & (data['prev_trend'] > 0), MeanInflectionPoint.factor_code] = -(abs(data['next_trend']) + abs(data['prev_trend']))/data[indicator_key]
        return data  
    
    def get_action_mapping(self, item):
        if (item[MeanInflectionPoint.factor_code] > 0):
            return 1
        elif (item[MeanInflectionPoint.factor_code] < 0):
            return -1
        else:
            return 0    
    
    # 指数平滑曲线    
    def score(self, data):
        # data = data.loc[len(data)-self._params[0]:len(data)-1]
        data = self.caculate(data)
        data['score'] = data[self._factor_code].ewm(com=1).mean()
        #最近一天的最小参数
        return data.iloc[len(data) - 1]['score']
    
# 移动平均线突破
class MeanPenetration(Factor):
    
    factor_code = 'mean_penetration'
    version = '2.1'
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = MovingAverage(self._params)
        data = indicator.enrich(data)
        indicator = ATREnvelope([20])
        data = indicator.enrich(data)
        open_indicator_key = 'mean.' + str(self._params[0])
        close_indicator_key = indicator.get_low_value_key(20)
        data['last_close'] = data['close'].shift(1)
        #只取穿透点
        data[MeanPenetration.factor_code] = 0
        data.loc[(data['close'] > data[open_indicator_key]) & (data['last_close'] < data[open_indicator_key]), MeanPenetration.factor_code] = data['close'] - data['last_close']
        data.loc[(data['close'] < data[close_indicator_key]) & (data['last_close'] > data[close_indicator_key]), MeanPenetration.factor_code] = data['close'] - data['last_close']
        # data.loc[(data['close'] < data[open_indicator_key]) & (data['last_close'] > data[open_indicator_key]), self._factor_code] = data['close'] - data['last_close']
        return data  
    
# 移动平均线趋势
class MeanTrend(Factor):
    
    factor_code = 'mean_trend'
    version = '1.0'
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = MovingAverage(self._params)
        data = indicator.enrich(data)
        indicator_key = 'mean.' + str(self._params[0])
        # 增量
        data[MeanTrend.factor_code] = (data[indicator_key] - data[indicator_key].shift(1))/data[indicator_key]
        return data   
    
    def get_action_mapping(self, item):
        if (item[MeanTrend.factor_code] > 0.01):
            return 1
        elif (item[MeanTrend.factor_code] <= 0):
            return -1
        else:
            return 0 
    
# 包络线突破因子基类
class EnvelopePenetration(Factor):
    
    _envelope_indicator = None
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        data = self._envelope_indicator.enrich(data)
        indicator = MovingAverage(self._params)
        data = indicator.enrich(data)
        open_indicator_key = self._envelope_indicator.get_high_value_key(self._envelope_indicator.get_params()[0])
        close_indicator_key = self._envelope_indicator.get_middle_value_key(self._params[0])
        data['last_close'] = data['close'].shift(1)
        #只取穿透点
        data[self.get_factor_code()] = 0
        data.loc[(data['close'] > data[open_indicator_key]) & (data['last_close'] < data[open_indicator_key]), self.get_factor_code()] = data['close'] - data['last_close']
        data.loc[(data['close'] < data[close_indicator_key]) & (data['last_close'] > data[close_indicator_key]), self.get_factor_code()] = data['close'] - data['last_close']
        return data  
    
    def get_envelope_indicator(self):
        return self._envelope_indicator
    
    
# 均值百分比包络线
class EnvelopePenetration_MeanPercentage(EnvelopePenetration):
    
    factor_code = 'EnvelopePenetration_MeanPercentage'
    
    def __init__(self, params):
        self._params = params
        self._envelope_indicator = MeanPercentageEnvelope(self._params)
        
# ATR
class EnvelopePenetration_ATR(EnvelopePenetration):
    
    factor_code = 'EnvelopePenetration_ATR'
    
    def __init__(self, params):
        self._params = params
        self._envelope_indicator = ATREnvelope(self._params)
 
'''
肯特纳突破
和标准包络突破相比，多加了一个条件：中线上升
'''        
class EnvelopePenetration_Keltner(EnvelopePenetration):
    
    _envelope_indicator = None
    factor_code = 'EnvelopePenetration_Keltner'
    
    def __init__(self, params):
        self._params = params
        self._envelope_indicator = KeltnerEnvelope(self._params)
    
    def caculate(self, data):
        data = self._envelope_indicator.enrich(data)
        open_indicator_key = self._envelope_indicator.get_high_value_key(self._params[0])
        close_indicator_key = self._envelope_indicator.get_middle_value_key(self._params[0])
        data.loc[:,'last_close'] = data['close'].shift(1)
        data.loc[:,'last_' + close_indicator_key] = data[close_indicator_key].shift(1)
        data.loc[:,'middle_trend'] = (data[close_indicator_key] - data['last_' + close_indicator_key]) / data[close_indicator_key]
        data.loc[:,EnvelopePenetration_Keltner.factor_code] = 0
        data.loc[(data['close'] > data[open_indicator_key]) & (data['middle_trend'] > 0.0) & (data['close'] > data['last_close']), EnvelopePenetration_Keltner.factor_code] = data['close'] - data['last_close']
        data.loc[(data['close'] < data[close_indicator_key]) & (data['last_close'] > data[close_indicator_key]), EnvelopePenetration_Keltner.factor_code] = data['close'] - data['last_close']
        return data.copy()
    
'''
改进肯特纳突破
'''    
class AdvanceEnvelopePenetration_Keltner(EnvelopePenetration_Keltner):
    
    factor_code = 'AdvanceEnvelopePenetration_Keltner'
    
    def __init__(self, params):
        self._params = params
        self._envelope_indicator = AdvanceKeltnerEnvelope(self._params)
        
if __name__ == '__main__':
    # #图像分析
    # 股票
    # data = FileUtils.get_file_by_ts_code('300462.SZ', is_reversion = True)
    # factor = MeanInflectionPoint([20])
    # # factor = MeanPenetration([20])
    # # factor = MeanTrend([20])
    # # factor = EnvelopePenetration_MeanPercentage([20])
    # # factor = EnvelopePenetration_ATR([20])
    # # factor = EnvelopePenetration_Keltner([20])
    # # factor = AdvanceEnvelopePenetration_Keltner([20])
    # data = factor.caculate(data)
    # data['index_trade_date'] = pd.to_datetime(data['trade_date'])
    # data = data.set_index(['index_trade_date'])
    # draw_analysis_curve(data[(data['trade_date'] >= '20210101')], volume = False, show_signal = True, signal_keys = [factor.get_factor_code(),'mean.20'])
    # print('aa')
    # 期货
    # data = FileUtils.get_file_by_product_and_instrument('IF', 'IF2204')
    # data = create_k_line('RB2210')
    # factor = MeanInflectionPoint([10])
    # # factor = MeanPenetration([20])
    # # factor = MeanTrend([20])
    # # factor = EnvelopePenetration_MeanPercentage([20])
    # # factor = EnvelopePenetration_ATR([20])
    # # factor = EnvelopePenetration_Keltner([20])
    # # factor = AdvanceEnvelopePenetration_Keltner([20])
    # data = factor.caculate(data)
    # draw_analysis_curve(data[(data.index >= '2022-04-11 10:40:00') & (data.index <= '2022-04-11 11:30:00')], volume = False, show_signal = True, signal_keys = [factor.get_factor_code()])
    # # draw_analysis_curve(data, volume = True, show_signal = True, signal_keys = [factor.get_factor_code(),'mean.20'])
    # print('aa')
    
    #模拟
    #股票
    # data = FileUtils.get_file_by_ts_code('300462.SZ', is_reversion = True)
    # # factor = LowerHatch([5])
    # # factor = MeanInflectionPoint([20])
    # # factor = MeanTrend([20])
    # factor = MeanPenetration([20])
    # # factor = EnvelopePenetration_MeanPercentage([20])
    # # factor = EnvelopePenetration_ATR([20])
    # # factor = EnvelopePenetration_Keltner([20])
    # simulator = StockSimulator()
    # simulator.simulate(factor, data, start_date = '20210101', save = False)
    # simulate(factor, data, start_date = '20210101', save = False)
    #期货
    # data = FileUtils.get_file_by_product_and_instrument('IH', 'IH2109')
    data = create_k_line('RB2210', directly_from_db=True)
    factor = MeanInflectionPoint([10])
    # factor = MeanTrend([20])
    # factor = MeanPenetration([20])
    # factor = EnvelopePenetration_MeanPercentage([20])
    # factor = EnvelopePenetration_ATR([20])
    # factor = EnvelopePenetration_Keltner([20])
    simulator = FutrueSimulator()
    simulator.simulate(factor, data, save = False)
    # simulator.print_action_matrix('RB2210', factor, data, only_action = False)
    # simulator.simulate(factor, data[(data.index >= '2022-04-11 10:40:00') & (data.index <= '2022-04-11 11:30:00')], save = False)
    
    
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
    
     # 测试 capital_curve_simulate
    # initial_capital_amount = 1000000
    # factor = MeanInflectionPoint([10])
    # capital_curve_simulate(initial_capital_amount, 50, factor, '2021-11-10 00:00:00', products = ['FU'])