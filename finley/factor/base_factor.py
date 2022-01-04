#! /usr/bin/env python
# -*- coding:utf8 -*-

from abc import ABCMeta, abstractclassmethod
import pandas as pd
from scipy.stats import pearsonr
    
'''
因子基类
'''
class Factor(metaclass = ABCMeta):
    
    _params = []
    _factor_code = ''
    _version = '1.0'
    _signal_delay = 1
    
    def get_factor_code(self):
        return self._factor_code
    
    def get_params(self):
        return self._params
    
    def get_signal_delay(self):
        return self._signal_delay
    
    def get_version(self):
        return self._version
    
    def score(self, data):
        # data = data.loc[len(data)-self._params[0]:len(data)-1]
        data = self.caculate(data)
        #最近一天的最小参数
        return data.iloc[len(data) - 1][self._factor_code]
    
    @staticmethod
    def caculate_ret(data, period):
        data["ret." + str(period)] = (data.shift(-period)["close"] - data["close"]) * 100 / data["close"]
        return data
    
    # 两个因子相关性分析
    def compare(self, factor, data):
        data = self.caculate(data)
        data = factor.caculate(data)
        data = data.dropna()
        return pearsonr(data[self.get_factor_code()], data[factor.get_factor_code()])
    
    # 多数据源计算并组合
    def caculate_concat(self, data_list, reset_index = True):
        total_data = pd.concat(data_list)
        if (reset_index):
            total_data = total_data.reset_index(drop=True)
        return total_data
    
    # 操作默认实现，1-开仓，-1-平仓
    def get_action_mapping(self, item):
        if (item[self.get_factor_code()] > 0):
            return 1
        elif (item[self.get_factor_code()] < 0):
            return -1
        else:
            return 0
    
    #全局计算因子值
    @abstractclassmethod
    def caculate(self, data):
        pass
    
        

    
        
