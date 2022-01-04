#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd

from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.decomposition import PCA

from persistence import DaoMysqlImpl, FileUtils
from factor.base_factor import Factor
from factor.my_factor import LowerHatch
from factor.trend_factor import MeanInflectionPoint, MeanPenetration, MeanTrend
from simulator import simulate
from visualization import draw_analysis_curve
from structrue import Sample
from tools import create_instance

class TrainingSet(object):
    
    def  __init__(self, object):
        self._ts_code = object['ts_code']
        self._start_date = object['start_date']
        self._end_date = object['end_date']
        
    def get_ts_code(self):
        return self._ts_code
    
    def get_start_date(self):
        return self._start_date
    
    def get_end_date(self):
        return self._end_date
        
class TrainingModel(object):
    
    def __init__(self, model_id):
        persistence = DaoMysqlImpl()
        self._model_id = model_id
        json_model = persistence.get_learning_model(self._model_id)
        training_set = json_model['training_set']
        self._training_set = [TrainingSet(elem) for elem in training_set] 
        self._profit_period = int(json_model['profit_period'])
        self._pre_process = []
        self._algorithm = create_instance('sklearn.linear_model', json_model['algorithm'])
        
    def get_training_set(self):
        return self._training_set
    
    def get_profit_period(self):
        return self._profit_period
    
    def get_pre_process(self):
        return self._pre_process
    
    def get_algorithm(self):
        return self._algorithm
    
    def get_model_id(self):
        return self._model_id
        
class MachineLearn(object):
    
    def __init__(self, factor_list, training_model):
        self._factor_list = factor_list
        self._training_model = training_model

    """
    @描述: 生成训练集
    ---------
    @参数: factor_list: 训练因子
        sample_list: 样本 
    -------
    @返回: 训练集
    -------
    """
    def create_train_set(self):
        X = pd.DataFrame()
        total_data = pd.DataFrame()
        for model in self._training_model.get_training_set():
            ts_code = model.get_ts_code()
            data = FileUtils.get_file_by_ts_code(ts_code, is_reversion=True)
            data = Factor.caculate_ret(data, self._training_model.get_profit_period())
            for factor in self._factor_list:
                data = factor.caculate(data)
            if (model.get_start_date() != ''):
                data = data[data['trade_date'] >= model.get_start_date()]
            if (model.get_end_date() != ''):
                data = data[data['trade_date'] <= model.get_end_date()]
            total_data = pd.concat([data])
            total_data = total_data.dropna()
            total_data = total_data.reset_index(drop=True)
        for factor in self._factor_list:
            X[factor.get_factor_code()] = total_data[factor.get_factor_code()]
        self._y = total_data['ret.' + str(self._training_model.get_profit_period())]
        self._X = X


    """
    @描述: 训练模型
    ---------
    @参数: data_set: Dataframe 待训练数据集
        algorithm_impl: class 算法实现
        ret_param: string 收益率参数 
    -------
    @返回: 模型
    -------
    """
    def train_model(self):
        self.create_train_set()
        X_train = self._X
        y_train = self._y
        if (len(self._training_model.get_pre_process()) > 0):
            for handler in self._training_model.get_pre_process():
                handler.fix(X_train)
                X_train = handler.transform(X_train)
        self._training_model.get_algorithm().fit(X_train, y_train)
        return { 'model' : self._training_model.get_algorithm(),
                'prehandler_list' : self._training_model.get_pre_process()}
        
    def get_train_set(self):
        return {
            'X' : self._X,
            'y' : self._y
        }
        
    def get_factor_list(self):
        return self._factor_list;
    
    def get_model_id(self):
        return self._training_model.get_model_id();

# 复合因子
class CompoundFactor(Factor):
    
    def __init__(self, machine_learn):
        self._machine_learn = machine_learn
        self._params = [0]
    
    def score(self, data):
        data = data.loc[len(data)-self._params[0]:len(data)-1]
        data = self.caculate(data)
        #最近一天的最小参数
        return data.iloc[len(data) - 1][self._factor_code]
    
    def caculate(self, data):
        factor_list = self._machine_learn.get_factor_list()
        for factor in factor_list:
            data = factor.caculate(data)
        data = data.dropna()
        X_train = pd.DataFrame()
        for factor in factor_list:
            X_train[factor.get_factor_code()] = data[factor.get_factor_code()]
        data[self.get_factor_code()] = self._machine_learn.train_model()['model'].predict(X_train)
        return data
    
    def get_factor_code(self):
        factor_code = ''
        for i, factor in enumerate(self._machine_learn.get_factor_list()):
            if i == 0:
                factor_code = factor.get_factor_code() + '_' + str(factor.get_params()[0])
            else:
                factor_code = factor_code + '_' + factor.get_factor_code() + '_' + str(factor.get_params()[0])
        return factor_code 
    
    def get_action_mapping(self, item):
        if (item[self.get_factor_code()] > 0):
            return 1
        elif (item[self.get_factor_code()] < 0):
            return -1
        else:
            return 0
    
    def get_params(self):
        param = ''
        for factor in self._machine_learn.get_factor_list():
            param = param + '_' + str(factor.get_params()[0])
        return [param]
    
    def get_model_id(self):
        return self._machine_learn.get_model_id()
        
    
if __name__ == '__main__':
    # 测试训练模型
    # model = TrainingModel(1)
    # print(model)
    
    # 生成训练集
    # factor_list = []
    # factor_list.append(MeanPenetration([20]))
    # factor_list.append(MeanTrend([20]))
    # training_model = TrainingModel(3)
    # learn = MachineLearn(factor_list, training_model)
    # model = learn.train_model()
    # train_set = learn.get_train_set()
    # print(model['model'].score(train_set['X'], train_set['y']))
    
    #图像分析
    data = FileUtils.get_file_by_ts_code('300366.SZ', is_reversion = True)
    factor_list = []
    factor_list.append(MeanPenetration([20]))
    factor_list.append(MeanTrend([20]))
    training_model = TrainingModel(3)
    learn = MachineLearn(factor_list, training_model)
    factor = CompoundFactor(learn)
    data = factor.caculate(data)
    factor1 = MeanPenetration([20])
    data = factor1.caculate(data)
    factor2 = MeanTrend([20])
    data = factor2.caculate(data)
    data['index_trade_date'] = pd.to_datetime(data['trade_date'])
    data = data.set_index(['index_trade_date'])
    draw_analysis_curve(data[(data['trade_date'] <= '20211106') & (data['trade_date'] > '20210101')], volume = False, show_signal = True, signal_keys = [factor.get_factor_code(), 'mean_penetration','mean_trend','mean.20'])
    print('aa')
    
    # 测试复合因子
    factor_list = []
    factor_list.append(MeanPenetration([20]))
    factor_list.append(MeanTrend([20]))
    training_model = TrainingModel(1)
    learn = MachineLearn(factor_list, training_model)
    factor = CompoundFactor(learn)
    data = FileUtils.get_file_by_ts_code('300366.SZ', is_reversion = True)
    simulate(factor, data, start_date = '20210101', save = False)