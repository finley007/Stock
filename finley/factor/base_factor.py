#! /usr/bin/env python
# -*- coding:utf8 -*-

import os,sys 
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir) 
from abc import ABCMeta, abstractclassmethod
import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import tools
from persistence import FileUtils, DaoMysqlImpl
from framework import Pagination
from parallel import ProcessRunner
    
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
    
    def get_key(self, param):
        """
        根据参数获取因子关键字
        """
        return self.factor_code + '.' + str(param)

    def get_keys(self):
        """
        获取所有因子关键字
        """
        return list(map(lambda param: self.factor_code + '.' + str(param), self._params))
    
    def get_signal(self, param):
        """
        根据参数获取信号量关键字
        """
        return self.factor_code + '.' + str(param) + '.signal'
    
    def get_signals(self):
        """
        获取所有信号量关键字
        """
        return list(map(lambda param: self.factor_code + '.' + str(param) + '.signal', self._params))
    
    def score(self, data, param=''):
        """
        根据新的设计重新实现
        """
        # data = data.loc[len(data)-self._params[0]:len(data)-1]
        data = self.caculate(data)
        #最近一天的最小参数
        if isinstance(self.get_params(),list):
            return data.iloc[len(data) - 1][self.get_key(param)]
        else:
            return data.iloc[len(data) - 1][self.get_key()]
    
    def analyze(self, stock_list=[], start_date = '', end_date = ''):
        """
        分析因子的值域，分布，均值
        """
        statistics_result = {}
        statistics_data = {}
        for param in self._params:
            factor_value_list = []
            if len(stock_list) == 0:
                persistence = DaoMysqlImpl()
                stock_list = persistence.get_stock_list()
            pagination = Pagination(stock_list, page_size=50)
            runner = ProcessRunner(10)
            while pagination.has_next():
                sub_list = pagination.next()
                runner.execute(self.get_factor_value_list, args = (param, sub_list, start_date, end_date))
                results = runner.get_results()
                for result in results:
                    factor_value_list = factor_value_list + result.get()
            factor_value_array = np.array(factor_value_list)
            ptile_array = np.percentile(factor_value_array, [10, 20, 30, 40, 50, 60, 70, 80, 90])
            statistics_result[param] = {
            'max' : np.amax(factor_value_array),
            'min' : np.amin(factor_value_array),
            'scope' : np.ptp(factor_value_array),
            'mean' : np.mean(factor_value_array),
            'median' : np.median(factor_value_array),
            'std' : np.std(factor_value_array),
            'var' : np.var(factor_value_array),
            'ptile10' : ptile_array[0],
            'ptile20' : ptile_array[1],
            'ptile30' : ptile_array[2],
            'ptile40' : ptile_array[3],
            'ptile50' : ptile_array[4],
            'ptile60' : ptile_array[5],
            'ptile70' : ptile_array[6],
            'ptile80' : ptile_array[7],
            'ptile90' : ptile_array[8]
            }
            statistics_data[param] = factor_value_list
        return statistics_result, statistics_data

    def get_factor_value_list(self, param, sub_list, start_date = '', end_date = ''):
        """
        计算因子值，为了多进程并行计算
        """
        for stock in sub_list:
            print('Handle stock: ' + stock)
            data = FileUtils.get_file_by_ts_code(stock)
            data = self.caculate(data)
            data = data.dropna()
            if start_date != '':
                data = data[data['date'] >= start_date]
            if end_date != '':
                data = data[data['date'] <= start_date]
        return data[self.get_key(param)].tolist()
    
    @classmethod
    def get_factor_code(clz):
        return clz.factor_code
    
    @classmethod
    def get_signal_delay(clz):
        return clz.signal_delay
    
    @classmethod
    def get_version(clz):
        return clz.version
    
    def parse_factor_case(clz, case):
        """
        解析factor case，格式：
        factorcode_version_params_threshold_starttime_endtime
        例子： MeanInflectionPoint_v1.0_5_0.8_20210101_20210929
        """
        return case.split('_')
    
    @staticmethod
    def caculate_ret(data, periods):
        for period in periods:
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
    def get_action_mapping(self, param, item):
        if (item[self.get_key(param)] > 0):
            return 1
        elif (item[self.get_key(param)] < 0):
            return -1
        else:
            return 0
    
    #全局计算因子值
    @abstractclassmethod
    def caculate(self, data, create_signal=True):
        pass
    
    #获取可视化参数列表
    def obtain_visual_monitoring_parameters(self):
        return [self.get_factor_code()]

class CombinedParamFactor(Factor):
    """
    组合参数因子，多个参数共同组合起来使用
    """

    def get_key(self):
        return self.factor_code + '.' + '.'.join(self.params_tostring())
    
    def get_signal(self):
        return self.factor_code + '.' + '.'.join(self.params_tostring()) + '.signal'
    
    def get_params(self):
        return '_'.join(self.params_tostring())
    
    def params_tostring(self):
        return list(map(lambda param: str(param), self._params))
    
    def analyze(self, stock_list=[], start_date = '', end_date = ''):
        """
        分析因子的值域，分布，均值
        """
        statistics_result = {}
        statistics_data = {}
        factor_value_list = []
        if len(stock_list) == 0:
            persistence = DaoMysqlImpl()
            stock_list = persistence.get_stock_list()
        pagination = Pagination(stock_list, page_size=50)
        runner = ProcessRunner(10)
        while pagination.has_next():
            sub_list = pagination.next()
            runner.execute(self.get_factor_value_list, args = (sub_list, start_date, end_date))
            results = runner.get_results()
            for result in results:
                factor_value_list = factor_value_list + result.get()
        factor_value_array = np.array(factor_value_list)
        ptile_array = np.percentile(factor_value_array, [10, 20, 30, 40, 50, 60, 70, 80, 90])
        statistics_result = {
        'max' : np.amax(factor_value_array),
        'min' : np.amin(factor_value_array),
        'scope' : np.ptp(factor_value_array),
        'mean' : np.mean(factor_value_array),
        'median' : np.median(factor_value_array),
        'std' : np.std(factor_value_array),
        'var' : np.var(factor_value_array),
        'ptile10' : ptile_array[0],
        'ptile20' : ptile_array[1],
        'ptile30' : ptile_array[2],
        'ptile40' : ptile_array[3],
        'ptile50' : ptile_array[4],
        'ptile60' : ptile_array[5],
        'ptile70' : ptile_array[6],
        'ptile80' : ptile_array[7],
        'ptile90' : ptile_array[8]
        }
        statistics_data = factor_value_list
        return statistics_result, statistics_data
    
    def get_factor_value_list(self, sub_list, start_date = '', end_date = ''):
        """
        计算因子值，为了多进程并行计算
        """
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

class CombinationFactor(Factor):
    
    def __init__(self, factor_code, factor_list, params_mapping={}):
        self._factor_list = factor_list
        self._factor_code = factor_code
        self._params_mapping = params_mapping
        
    def append_factor(self, factor):
        self._factor_list.append(factor)
        
    def get_signal(self):
        return self.factor_code + '.signal'

    def get_params(self):
        return ''
        
    def caculate(self, data):
        for factor in self._factor_list:
            data = factor.caculate(data)
        data[self.get_signal()] = 1
        for factor in self._factor_list:
            if isinstance(factor.get_params(),list):
                data[self.get_signal()] = np.array(data[factor.get_signal(self._params_mapping[factor.get_factor_code()])]) * data[self.get_signal()]
            else:
                mask = data[factor.get_signal()].tolist()
                print(data[self.get_signal()].tolist())
                print(mask)
                data[self.get_signal()] = np.array(mask) * data[self.get_signal()]
                print(data[self.get_signal()].tolist())
        return data
    
    def score(self, data):
        data = self.caculate(data)
        score = 0
        scores = [1, 4, 9, 16, 25]
        signals = data[self.get_signal()].tolist()
        if len(signals) >= 5:
            score = np.dot(scores, signals[-5:])
        return score
        
    
        
if __name__ == '__main__':
    print(Factor.get_factor_by_code('factor.momentum_factor','kdj_regression'))
        

    
        
