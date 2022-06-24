#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd
import time
import ray
from factor.base_factor import Factor
ray.init()

from factor.my_factor import LowerHatch
from factor.trend_factor import MeanInflectionPoint, MeanTrend, MeanPenetration, MeanTrendFirstDiff
from factor.momentum_factor import KDJRegression, RSIPenetration, DRFPenetration, SOPenetration
from factor.volume_factor import MFIPenetration, OBVTrend
from persistence import DaoMysqlImpl, FileUtils
from analysis import correlation_analysis, select_stock, retro_select_stock, position_analysis
from visualization import draw_histogram
from synchronization import incremental_synchronize_stock_daily_data, synchronize_all_stock, synchronize_stock_daily_data
from simulator import StockSimulator, FutrueSimulator 
from machinelearning import MachineLearn, CompoundFactor, TrainingModel
from log import log_info
from tools import run_with_timecost, create_instance, to_params, date_to_time
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
def do_correlation_analysis(factor):
    data = FileUtils.get_file_by_ts_code('603882.SH', is_reversion = True)
    periods = [1,2,3,4,5]
    correlation_analysis(factor, data, periods)
    draw_histogram(data[factor.get_factor_code()], 50)
    print("aa")

@run_with_timecost    
def run_single_factor_simulation(package, factor_case_code, ts_code = '', is_stock = True):
    persistence = DaoMysqlImpl()
    factor_case = [('_'+factor_case_code).split('_')]
    # factor_case = persistence.select("select * from factor_case where id = '" + factor_case_code + "'")
    factor = create_instance(package, factor_case[0][1], to_params(factor_case[0][2]))
    if (is_stock):
        simulator = StockSimulator()
        if (ts_code != ''):
            stock_list = persistence.select("select ts_code from static_stock_list")
        else:
            stock_list = persistence.select("select ts_code from static_stock_list where ts_code = '" + ts_code + "'")
        for stock in stock_list:
            data = FileUtils.get_file_by_ts_code(stock[0], is_reversion = True)
            simulator.simulate(factor, data, factor_case[0][3])
    else:
        simulator = FutrueSimulator()
        if (ts_code == ''):
            start_time = date_to_time(factor_case[0][3])
            instrument_list = persistence.select("select product, instrument from future_instrument_list where instrument not in (select ts_code from simulation_result where type = 'FUTURE') and start_time >= '" + start_time + "'")
        else:
            instrument_list = persistence.select("select product, instrument from future_instrument_list where instrument = '" + ts_code + "'")
        for instrument in instrument_list:
            data = FileUtils.get_file_by_product_and_instrument(instrument[0], instrument[1], True)
            simulator.simulate(factor, data)
    # time.sleep(300)
  
@run_with_timecost      
def run_compound_factor_simulation(factor_list, model_id, start_date, ret_period = 5):
    persistence = DaoMysqlImpl()
    training_model = TrainingModel(1)
    learn = MachineLearn(factor_list, training_model)
    factor = CompoundFactor(learn)
    stock_list = persistence.select("select ts_code from static_stock_list")
    for stock in stock_list:
        data = FileUtils.get_file_by_ts_code(stock[0], is_reversion = True)
        # simulate(factor, data, start_date)
        
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
    pre_check()
    # 相关性分析
    # factor = OBVTrend([0])
    # do_correlation_analysis(factor)
    # # 参数调优
    # params = [4, 6, 8, 10, 12, 14, 16, 18, 20 ,22, 24, 26, 28 ,30,32,34,36,38,40]
    # ts_code = 'IF2204'
    # for period in params:
    #     code = "MeanInflectionPoint_{period}".format(period=period)
    #     run_single_factor_simulation('factor.trend_factor', code, ts_code, False)
        # print(code)
    # 单一因子模拟
    # run_single_factor_simulation('factor.trend_factor', 'MeanTrendFirstDiff_10_20210101_20220614', False)
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
    # factor = MFIPenetration([14])
    # factor = MeanTrendFirstDiff([10])
    # data = select_stock([factor])
    # 复盘
    # factor1 = KDJRegression([9])
    # factor2 = RSIPenetration([14])
    factor = MeanTrendFirstDiff([10])
    # data = run_retro_select_stock([factor1, factor2], '20220218')
    data = run_retro_select_stock([factor], '20220614')
    # 持股分析
    # run_position_analysis('factor.momentum_factor')
