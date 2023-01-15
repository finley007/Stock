#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd
import time
from factor.base_factor import Factor
import os
import uuid
import numpy as np

from factor.my_factor import LowerHatch
from factor.trend_factor import MeanInflectionPoint, MeanTrend, MeanPenetration, MeanTrendFirstDiff, MultipleMeanPenetration
from factor.momentum_factor import KDJRegression, RSIPenetration, DRFPenetration, SOPenetration
from factor.volume_factor import MFIPenetration, OBVTrend, FIPenetration
from filter import NewStockFilter, STFilter, create_filter_list, filter_stock
from persistence import DaoMysqlImpl, FileUtils, create_session, FactorAnalysis, DistributionResult, FactorRetDistribution
from analysis import correlation_analysis, select_stock, retro_select_stock, position_analysis
from visualization import draw_histogram
from synchronization import incremental_synchronize_stock_daily_data, synchronize_all_stock, synchronize_stock_daily_data
from simulator import StockSimulator, FutrueSimulator 
from machinelearning import MachineLearn, CompoundFactor, TrainingModel
from log import log_info
from tools import run_with_timecost, create_instance, to_params, date_to_time
from validator import validate_data_integrity
import constants



@run_with_timecost
def pre_check():
    '''
    1. 更新股票列表
    2. 检查所有股票数据，如果有为空的则自动同步
    3. 检查所有股票数据是否有NA，如果有则增量同步
    4. 检查所有股票数据是否更新到最新的数据
    '''
    #1
    synchronize_all_stock()
    
    #2
    validate_data_integrity()
    
@run_with_timecost   
def do_correlation_analysis(factor):
    data = FileUtils.get_file_by_ts_code('603882.SH', is_reversion = True)
    periods = [1,2,3,4,5]
    correlation_analysis(factor, data, periods)
    draw_histogram(data[factor.get_factor_code()], 50)
    print("aa")
    
@run_with_timecost   
def return_distribution_statistics(factor):
    data = FileUtils.get_file_by_ts_code('603882.SH', is_reversion = True)
    periods = [1,2,3,4,5]
    for period in periods:
        Factor.caculate_ret(data, period)
    data = factor.caculate(data)

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
    
@run_with_timecost    
def run_factor_analysis(package, factor_case_exp, filters, ts_code = ''):
    """
    分析因子值的分布
    """
    persistence = DaoMysqlImpl()
    session = create_session()
    factor_case = parse_factor_case(factor_case_exp)
    factor = create_instance(package, factor_case[0], to_params(factor_case[2]))
    filter_stock_list = []
    if ts_code == '':
        filter_list = create_filter_list(filters)
        stock_list = persistence.select("select ts_code from static_stock_list")
        stock_list = list(map(lambda item:item[0], stock_list))
        for stock in stock_list:
            data = FileUtils.get_file_by_ts_code(stock, is_reversion = True)
            if (len(data) > 0 and len(filter_list) > 0 and filter_stock(filter_list, data)):
                filter_stock_list.append(stock)
    else:
        stock_list = persistence.select("select ts_code from static_stock_list where ts_code = '" + ts_code + "'")
        filter_stock_list = list(map(lambda item:item[0], stock_list))
    result = factor.analyze(filter_stock_list)
    for param in factor.get_params():
        factor_analysis = FactorAnalysis(factor_case_exp, filters, param)
        session.add(factor_analysis)
        file_name = str(uuid.uuid4()).replace('-','')
        path = constants.REPORT_PATH + os.path.sep + 'factor_analysis' + os.path.sep + str(file_name) + '.pkl'
        FileUtils.save(result[1][param], path)
        distribution_result = DistributionResult(0, factor_analysis.get_id(), result[0][param], path)
        session.add(distribution_result)
        session.commit()
    
@run_with_timecost    
def run_factor_ret_distribution_analysis(package, factor_case_exp, filters, ts_code = ''):
    """
    分析因子值收益率的分布
    """
    ret_schema_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    persistence = DaoMysqlImpl()
    session = create_session()
    factor_case = parse_factor_case(factor_case_exp)
    factor = create_instance(package, factor_case[0], to_params(factor_case[2]))
    filter_stock_list = []
    if ts_code == '':
        filter_list = create_filter_list(filters)
        stock_list = persistence.select("select ts_code from static_stock_list")
        stock_list = list(map(lambda item:item[0], stock_list))
        for stock in stock_list:
            data = FileUtils.get_file_by_ts_code(stock, is_reversion = True)
            if (len(data) > 0 and len(filter_list) > 0 and filter_stock(filter_list, data)):
                filter_stock_list.append(stock)
    else:
        stock_list = persistence.select("select ts_code from static_stock_list where ts_code = '" + ts_code + "'")
        filter_stock_list = list(map(lambda item:item[0], stock_list))
    data = pd.DataFrame()
    for stock in filter_stock_list:
        temp_data = FileUtils.get_file_by_ts_code(stock, is_reversion = True)
        temp_data = factor.caculate(temp_data)
        temp_data = Factor.caculate_ret(temp_data, ret_schema_list)
        temp_data = temp_data.dropna()
        data = pd.concat([data, temp_data])
    for param in factor.get_params():
        ret_id = []
        for ret in ret_schema_list:
            ret_list = data[data[factor.get_signal(param)] == 1]['ret.' + str(ret)].tolist()
            ret_ptile_array = np.percentile(ret_list, [10, 20, 30, 40, 50, 60, 70, 80, 90])
            ret = np.array(ret_list)
            result = {
                    'max' : np.amax(ret),
                    'min' : np.amin(ret),
                    'scope' : np.ptp(ret),
                    'mean' : np.mean(ret),
                    'median' : np.median(ret),
                    'std' : np.std(ret),
                    'var' : np.var(ret),
                    'ptile10' : ret_ptile_array[0],
                    'ptile20' : ret_ptile_array[1],
                    'ptile30' : ret_ptile_array[2],
                    'ptile40' : ret_ptile_array[3],
                    'ptile50' : ret_ptile_array[4],
                    'ptile60' : ret_ptile_array[5],
                    'ptile70' : ret_ptile_array[6],
                    'ptile80' : ret_ptile_array[7],
                    'ptile90' : ret_ptile_array[8]
            }
            related_id = str(uuid.uuid4()).replace('-','')
            ret_id.append(related_id)
            file_name = str(uuid.uuid4()).replace('-','')
            path = constants.REPORT_PATH + os.path.sep + 'factor_ret_distribution' + os.path.sep + str(file_name) + '.pkl'
            FileUtils.save(ret_list, path)
            distribution_result = DistributionResult(1, related_id, result, path)
            session.add(distribution_result)
        factor_ret_distribution = FactorRetDistribution(factor_case, filters, ret_id, param)
        session.add(factor_ret_distribution)
        session.commit()
        
def parse_factor_case(factor_case):
    """
    解析factor case，格式：
    factorcode_version_params_threshold_starttime_endtime
    例子： MeanInflectionPoint_v1.0_5_0.8_20210101_20210929
    """
    return factor_case.split('_')
        
        
if __name__ == '__main__':
    pre_check()
    # 相关性分析
    # factor = OBVTrend([0])
    # do_correlation_analysis(factor)
    # 因子分析
    run_factor_analysis('factor.my_factor', 'RisingTrend_v1.0_5|10_0.8|0.7__', 'PriceFilter_50|STFilter')
    # run_factor_analysis('factor.my_factor', 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__', 'PriceFilter_50|STFilter')
    # run_factor_analysis('factor.my_factor', 'LowerHatch_v1.0_10_0.7__', 'PriceFilter_50|STFilter')
    # 因子收益率分布分析
    # run_factor_ret_distribution_analysis('factor.my_factor', 'RisingTrend_v1.0_5|10_0.8|0.7__', 'PriceFilter_50|STFilter')
    # run_factor_ret_distribution_analysis('factor.my_factor', 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__', 'PriceFilter_50|STFilter')
    # run_factor_ret_distribution_analysis('factor.my_factor', 'LowerHatch_v1.0_10_0.7__', 'PriceFilter_50|STFilter')
    # 概率分布分析
    # factor = OBVTrend([0])
    # # 参数调优
    # params = [4, 6, 8, 10, 12, 14, 16, 18, 20 ,22, 24, 26, 28 ,30,32,34,36,38,40]
    # ts_code = 'IF2204'
    # for period in params:
    #     code = "MeanInflectionPoint_{period}".format(period=period)
    #     run_single_factor_simulation('factor.trend_factor', code, ts_code, False)
        # print(code)
    # 单一因子模拟
    # run_single_factor_simulation('factor.volume_factor', 'FIPenetration_26_20210101_20220812', False)
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
    # factor = FIPenetration([26])
    # factor = MultipleMeanPenetration([10, 20])
    # data = select_stock([factor])
    # 复盘
    # factor1 = KDJRegression([9])
    # factor2 = RSIPenetration([14])
    # factor = MeanTrendFirstDiff([10])
    # data = run_retro_select_stock([factor1, factor2], '20220726')
    # data = run_retro_select_stock([factor], '20220720')
    # 持股分析
    # run_position_analysis('factor.momentum_factor')
