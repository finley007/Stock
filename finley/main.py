#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd
import time
import ray
from factor.base_factor import Factor
ray.init()

from factor.my_factor import LowerHatch
from factor.trend_factor import MeanInflectionPoint, MeanTrend, MeanPenetration
from factor.momentum_factor import KDJRegression, RSIPenetration, DRFPenetration, SOPenetration
from persistence import DaoMysqlImpl, FileUtils
from analysis import correlation_analysis, select_stock, retro_select_stock, position_analysis
from visualization import draw_histogram
from synchronization import incremental_synchronize_stock_daily_data, synchronize_all_stock, synchronize_stock_daily_data
from simulator import simulate 
from machinelearning import MachineLearn, CompoundFactor, TrainingModel
from log import log_info
from tools import run_with_timecost, create_instance, to_params
from validator import validate_data_integrity


'''
1. 更新股票列表
2. 检查所有股票数据，如果有为空的则自动同步
3. 检查所有股票数据是否有NA，如果有则增量同步
4. 检查所有股票数据是否更新到最新的数据
'''
@run_with_timecost
def pre_check():
    persistence = DaoMysqlImpl()
    stock_list = persistence.select("select ts_code from static_stock_list")
    
    #1
    # log_info('Update stock list')
    synchronize_all_stock()
    
    #2
    log_info('Validate data')
    validate_data_integrity()
 
@run_with_timecost   
def do_correlation_analysis():
    factor = LowerHatch([5])
    data = FileUtils.get_file_by_ts_code('603882.SH', is_reversion = True)
    periods = [1,2,3,4,5]
    correlation_analysis(factor, data, periods)
    draw_histogram(data[factor.get_factor_code()], 50)
    print("aa")

@run_with_timecost    
def run_single_factor_simulation(package, factor_case_code):
    persistence = DaoMysqlImpl()
    factor_case = persistence.select("select * from factor_case where id = '" + factor_case_code + "'")
    factor = create_instance(package, factor_case[0][1], to_params(factor_case[0][2]))
    stock_list = persistence.select("select ts_code from static_stock_list")
    for stock in stock_list:
        data = FileUtils.get_file_by_ts_code(stock[0], is_reversion = True)
        simulate(factor, data, factor_case[0][3])
  
@run_with_timecost      
def run_compound_factor_simulation(factor_list, model_id, start_date, ret_period = 5):
    persistence = DaoMysqlImpl()
    training_model = TrainingModel(1)
    learn = MachineLearn(factor_list, training_model)
    factor = CompoundFactor(learn)
    stock_list = persistence.select("select ts_code from static_stock_list")
    for stock in stock_list:
        data = FileUtils.get_file_by_ts_code(stock[0], is_reversion = True)
        simulate(factor, data, start_date)
        
@run_with_timecost    
def run_position_analysis(package):
    persistence = DaoMysqlImpl()
    transation_record = persistence.select("select * from transation_record where sell_date = ''")
    if (len(transation_record) > 0):
        for record in transation_record:
            factor_code = record[7]
            factorClz = Factor.get_factor_by_code(package, factor_code)
            param_value = record[9]
            factor = create_instance(package, factorClz.__name__, to_params(param_value))
            ts_code = record[1]
            open_price = record[3]
            open_date = record[4]
            volume = record[2]
            position_analysis(factor, ts_code, open_date, open_price, volume)
  
@run_with_timecost      
def run_select_stock(factor):
    select_stock(factor)
    
@run_with_timecost      
def run_retro_select_stock(factor_list, create_date):
    retro_select_stock(factor_list, create_date)
        
        
if __name__ == '__main__':
    # pre_check()
    # 相关性分析
    # do_correlation_analysis()
    # 单一因子模拟
    # run_single_factor_simulation('factor.momentum_factor', 'RVIPenetration_10_20210101_20220117')
    # 复合因子模拟
    # factor_list = []
    # factor_list.append(MeanPenetration([20]))
    # factor_list.append(MeanTrend([20]))
    # run_compound_factor_simulation(factor_list, '1', '20210101')
    # 选股
    # factor1 = KDJRegression([9])
    # factor2 = RSIPenetration([14])
    # data = select_stock([factor1, factor2])
    # factor = SOPenetration([10])
    # data = select_stock([factor])
    # 复盘
    # factor1 = KDJRegression([9])
    # factor2 = RSIPenetration([14])
    # data = run_retro_select_stock([factor1, factor2], '20211231')
    # 持股分析
    run_position_analysis('factor.momentum_factor')
