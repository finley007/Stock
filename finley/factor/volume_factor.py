#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd

import os,sys 
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir) 
from persistence import FileUtils
from visualization import draw_analysis_curve
from indicator import FI, MFI, OBV
from simulator import StockSimulator, FutrueSimulator, capital_curve_simulate, SimulationConfig
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
    
# 资金流量突破
class MFIPenetration(Factor):
    
    factor_code = 'mfi_penetration'
    version = '1.0'
    
    _high_limit = 80
    _low_limit = 20
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = MFI(self._params)
        indicator.enrich(data)
        #只取穿透点
        data[MFIPenetration.factor_code] = 0
        #向上突破标记线买入
        data.loc[(data['mfi.'+str(self._params[0])].shift(1) < self._low_limit) & (data['mfi.'+str(self._params[0])] > self._low_limit), MFIPenetration.factor_code] = 100
        #向下突破标记线卖出
        data.loc[(data['mfi.'+str(self._params[0])].shift(1) > self._high_limit) & (data['mfi.'+str(self._params[0])] < self._high_limit), MFIPenetration.factor_code] = -100
        return data 
    
# 能量潮趋势
class OBVTrend(Factor):
    
    factor_code = 'obv_trend'
    version = '1.0'
    
    def __init__(self, params):
        self._params = params
    
    def caculate(self, data):
        indicator = OBV(self._params)
        indicator.enrich(data)
        data[OBVTrend.factor_code] = data['obv']
        return data 
    
     
if __name__ == '__main__':
    # #图像分析
    # 股票
    data = FileUtils.get_file_by_ts_code('600438.SH', is_reversion = True)
    factor = FIPenetration([26])
    data = factor.caculate(data)
    data['index_trade_date'] = pd.to_datetime(data['trade_date'])
    data = data.set_index(['index_trade_date'])
    data['volume'] = data['vol']
    draw_analysis_curve(data[(data['trade_date'] >= '20220101')], volume = True, show_signal = True, signal_keys = [factor.get_factor_code(),'fi.26'])
    print('aa')
    # 期货
    # data = FileUtils.get_file_by_product_and_instrument('IF', 'IF2204')
    # # data = create_k_line('RB2210')
    # factor = FIPenetration([26])
    # data = factor.caculate(data)
    # draw_analysis_curve(data[(data.index >= '2022-03-09 10:40:00') & (data.index <= '2022-03-15 11:30:00')], volume = False, show_signal = True, signal_keys = [factor.get_factor_code(), 'mean.20'])
    # # draw_analysis_curve(data, volume = True, show_signal = True, signal_keys = [factor.get_factor_code(),'mean.20'])
    # print('aa')
    
    #模拟
    #股票
    data = FileUtils.get_file_by_ts_code('002415.SZ', is_reversion = True)
    factor = FIPenetration([26])
    simulator = StockSimulator()
    simulator.simulate(factor, data, start_date = '20220101', save = False)
    #期货
    # data = FileUtils.get_file_by_product_and_instrument('IF', 'IF2204')
    # data['time'] = data.index
    # # data = create_k_line('RB2210', directly_from_db=True)
    # factor = FIPenetration([26])
    # simulator = FutrueSimulator()
    # config = SimulationConfig()
    # config.set_reverse_open(False)
    # simulator.simulate(factor, data, save = False, config = config)
    # simulator.print_action_matrix('FU2205', factor, data[(data.index >= '2021-11-10 21:00:00') & (data.index <= '2021-11-15 15:15:00')], only_action = False)
    # simulator.simulate(factor, data[(data.index >= '2022-03-09 10:40:00') & (data.index <= '2022-03-15 11:30:00')], save = False, config = config)
    
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
    