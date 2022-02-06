#! /usr/bin/env python
# -*- coding:utf8 -*-

import os,sys 
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir) 
from abc import ABCMeta, abstractclassmethod
import pandas as pd
from scipy.stats import pearsonr
import tools
    
'''
因子基类
'''
class Factor(metaclass = ABCMeta):
    
    factor_code = ''
    version = '1.0'
    signal_delay = 1
    
    _params = []
    
    def get_params(self):
        return self._params
    
    def score(self, data):
        # data = data.loc[len(data)-self._params[0]:len(data)-1]
        data = self.caculate(data)
        #最近一天的最小参数
        return data.iloc[len(data) - 1][self.get_factor_code()]
    
    @classmethod
    def get_factor_code(clz):
        return clz.factor_code
    
    @classmethod
    def get_signal_delay(clz):
        return clz.signal_delay
    
    @classmethod
    def get_version(clz):
        return clz.version
    
    @staticmethod
    def caculate_ret(data, period):
        data["ret." + str(period)] = (data.shift(-period)["close"] - data["close"]) * 100 / data["close"]
        return data
    
    @staticmethod
    def get_factor_by_code(module_name, code):
        class_list = tools.get_all_class(module_name)
        if (len(class_list) > 0):
            for clz in class_list:
                if (hasattr(clz, 'factor_code')):
                    print(getattr(clz, 'factor_code'))
                    if (getattr(clz, 'factor_code') == code):
                        return clz
        return None
    
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
    
if __name__ == '__main__':
    print(Factor.get_factor_by_code('factor.momentum_factor','kdj_regression'))
        

    
        
