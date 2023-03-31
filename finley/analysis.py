#! /usr/bin/env python
# -*- coding:utf8 -*-

from string import Template
import uuid

import pandas as pd
import numpy as np
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression, Ridge, Lasso

import constants
from tools import run_with_timecost, get_current_date, open_url
from persistence import DaoMysqlImpl, FileUtils
from visualization import draw_histogram
from filter import PriceFilter, STFilter, PERatioFilter, NewStockFilter, create_filter_list, filter_stock
from factor.base_factor import Factor
from factor.trend_factor import MeanTrend, EnvelopePenetration_Keltner, AdvanceEnvelopePenetration_Keltner, MeanPenetration, MeanInflectionPoint
from factor.momentum_factor import KDJRegression, RSIRegression, DRFRegression, WRRegression, UOPenetration
from factor.volume_factor import FIPenetration, MFIPenetration
from machinelearning import MachineLearn, CompoundFactor, TrainingModel
from simulator import Action
from datasource import TushareDatasource

def _ts_code_transform(orignal_str): 
    return orignal_str.split('.')[1].lower() + orignal_str.split('.')[0]


# 选股
@run_with_timecost
def select_stock(factor_list, stock_list = [], param_mapping = {}, filter_exp = '', save_result = True, open_link = True, last_business_date = ''):
    persistence = DaoMysqlImpl()
    if (len(stock_list) == 0):
        stock_list = persistence.select("select ts_code from static_stock_list")
    else:
        save_result = False
    score = []
    ts_code = []
    factor_code_list = []
    for factor in factor_list:
        factor_code_list.append(factor.get_factor_code())
    if (last_business_date == ''):
        last_business_date = persistence.get_last_business_date()
    filter_list = create_filter_list(filter_exp)
    for stock in stock_list:
        print("Handle stock: " + stock[0])
        data = FileUtils.get_file_by_ts_code(stock[0], True)
        if (len(data) > 0 and filter_stock(filter_list, data)):
            data = data[data['trade_date'] <= last_business_date]
            temp = []
            for factor in factor_list:
                if isinstance(factor.get_params(),list):
                    temp.append(factor.score(data, param_mapping[factor.get_factor_code()]))
                else:
                    temp.append(factor.score(data))
            score.append(temp)
            ts_code.append(stock[0])
    score_matrix = pd.DataFrame(score, columns=factor_code_list, index=ts_code)
    #归一化
    for factor_code in factor_code_list:
        score_matrix[factor_code] = (score_matrix[factor_code] - score_matrix[factor_code].min())/(score_matrix[factor_code].max() - score_matrix[factor_code].min())
    #加权平均
    score_matrix['score'] = 0
    for factor_code in factor_code_list:
        score_matrix['score'] = score_matrix[factor_code] + score_matrix['score']
    score_matrix['ts_code'] = score_matrix.index
    if (len(score_matrix) > 0):
        score_matrix = score_matrix.sort_values(by='score')
        score_matrix = score_matrix[score_matrix['score'].notnull()]
        score_matrix = score_matrix.iloc[len(score_matrix) - int(constants.ANALYSIS_RESULT_TOP_N): len(score_matrix)]
        if (save_result): 
            anaylysis_id = uuid.uuid1()
            for stock in score_matrix.itertuples(): 
                new_ts_code = _ts_code_transform(getattr(stock,'ts_code'))
                link = Template(constants.STOCK_INFO_LINK).safe_substitute(ts_code = new_ts_code)
                item = (anaylysis_id, create_factor_code(factor_list), getattr(stock,'score'), create_factor_params(factor_list, param_mapping), getattr(stock,'ts_code'), link, last_business_date,'','','','','')
                persistence.insert('insert into analysis_result values (REPLACE(UUID(),"-",""),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [item])
            if (open_link):
                open_selected_stocks_link(anaylysis_id)
        return score_matrix
    
# 平仓判断
@run_with_timecost
def position_analysis(factor, ts_code, open_date, open_price, volume, is_reversion = True):
    persistence = DaoMysqlImpl()
    last_business_date = persistence.get_last_business_date()
    data = FileUtils.get_file_by_ts_code(ts_code, True)
    if (len(data) > 0):
        datasource = TushareDatasource()
        data = factor.caculate(data)
        data.loc[:,'action'] = data.apply(lambda item:factor.get_action_mapping(item),axis=1)
        data = data[data['trade_date'] >= str(open_date)]
        reverse_factor = datasource.reverse_factor(ts_code, open_date)
        # 买入价格复权
        open_price = (float)(open_price) * reverse_factor['adj_factor'].iloc[0]
        action = Action(open_date, open_price, data) 
        sell_action_list = data[data['action'] == -1]
        close_date = last_business_date
        if (len(sell_action_list) > 0):
            close_date = sell_action_list[0]
        action.set_close_date(close_date, data)
        action.set_close_price(data[data['trade_date'] == close_date]['low'])
        print('+++++++++++++++++++++')
        print(action.get_open_date() + '-' + action.get_close_date())
        print(str(action.get_open_price()) + '-' + str(action.get_close_price()))
        profit = (action.get_close_price() - action.get_open_price()) * (float)(volume)
        profit_rate = profit/(action.get_open_price() * (float)(volume))
        print('利润：' + str(profit))
        print('利润率：' + str(profit_rate))
    
def create_factor_code(factor_list):
    return '_'.join(map(lambda x: str(x.get_factor_code()), factor_list))
                    
def create_factor_params(factor_list, param_mapping):
    params_detail = ''
    for factor in factor_list:
        if isinstance(factor.get_params(),list):
            params_detail = params_detail + param_mapping[factor.get_factor_code()] + '|'
        else:
            params_detail = params_detail + '|'
    return params_detail
 
# 打开选股结果
def open_selected_stocks_link(anlysis_id):
    persistence = DaoMysqlImpl()
    link_list = persistence.select("select ts_link from analysis_result where analysis_id = '" + str(anlysis_id) + "'")
    if (len(link_list) > 0):
        for link in link_list:
            open_url(link[0])

# 复盘
def retro_select_stock(factor_list, create_date):
    persistence = DaoMysqlImpl()
    result_list = persistence.select("select * from analysis_result where factor_code = '" + create_factor_code(factor_list) + "' and param_value = '" + create_factor_params(factor_list) + "' and create_date = '" + create_date + "'")
    for result in result_list:
        ts_code = result[5]
        data = FileUtils.get_file_by_ts_code(ts_code)
        tgt_data = data[data['trade_date'] >= create_date]
        for index in range(5):
            tgt_data['ret' + str(-(index + 1))] = (tgt_data.shift(-(index + 1))['close'] - tgt_data['close'])/tgt_data['close']
        list = []
        for index in range(5):
            ret = tgt_data[tgt_data['trade_date'] == create_date]['ret' + str(-(index + 1))]
            ret = ret.iloc[0] if not np.isnan(ret.iloc[0]) else ''
            list.append(ret)
        list.append(create_factor_code(factor_list))
        list.append(create_factor_params(factor_list))
        list.append(ts_code)
        list.append(create_date)
        persistence.update('update analysis_result set 1st_day_ret = %s, 2nd_day_ret = %s, 3rd_day_ret = %s, 4th_day_ret = %s, 5th_day_ret = %s where factor_code = %s and param_value = %s and ts_code = %s and create_date = %s', tuple(list))
 
# 全部股票
def correlation_analysis_all_stocks(factor, stock_list = [], save_result = True):
    persistence = DaoMysqlImpl()
    if (len(stock_list) == 0):
        stock_list = persistence.select("select ts_code from static_stock_list")
    else:
        save_result = False
    for stock in stock_list:
        print("Handle stock: " + stock[0])
        data = FileUtils.get_file_by_ts_code(stock[0], True)
        if (_filter_stock([NewStockFilter([30])], data)):
            correlation_analysis(factor, data, save_result = save_result)
     
# 相关性分析  
def correlation_analysis(factor, data, periods = range(1, 11), low_threshold = 0, high_threshold = 0, save_result = False):
    for period in periods:
        Factor.caculate_ret(data, period)
    data = factor.caculate(data)
    data = data.dropna()
    if (low_threshold > 0):
        data = data[data[factor.get_factor_code()] > low_threshold]
    if (high_threshold > 0):
        data = data[data[factor.get_factor_code()] < high_threshold]
    correlation_result = []
    valid_data = True
    for period in periods:
        if (len(data) > 1):
            correlation = pearsonr(data[factor.get_factor_code()], data['ret.' + str(period)])
            print(correlation)
            correlation_result.append('{:.8f}'.format(correlation[0]))
        else:
            valid_data = False
            break
    if (save_result and valid_data): 
        persistence = DaoMysqlImpl()
        ts_code = data['ts_code'].head(1).item()
        new_ts_code = _ts_code_transform(ts_code)
        link = Template(constants.STOCK_INFO_LINK).safe_substitute(ts_code = new_ts_code)
        model_id = get_compound_factor_model_id(factor)
        param = factor.get_params()[0]
        if (model_id != ''):
            param = model_id
        item = (factor.get_factor_code(), param, ts_code, link, get_current_date(),correlation_result[0],correlation_result[1],correlation_result[2],correlation_result[3],correlation_result[4],correlation_result[5],correlation_result[6],correlation_result[7],correlation_result[8],correlation_result[9])
        persistence.insert('insert into correlation_result values (REPLACE(UUID(),"-",""),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [item])

def get_compound_factor_model_id(factor):
    method = getattr(factor, "get_model_id", None)
    if callable(method):
        return method()
    else:
        return ''
    
'''
    相关性分析结果统计报告
    最大相关性，最大相关性股票代码
    最小相关性，最小相关性股票代码
    相关性均值
    相关性方差
    相关性分布
'''
def correlation_analysis_report(factor):
    persistence = DaoMysqlImpl()
    param = get_compound_factor_model_id(factor)
    if ('' == param):
        param = factor.get_params()[0]
    item = (factor.get_factor_code(), param)
    analysis_result = persistence.select('select * from correlation_result where factor_code = %s and param_value = %s', item)
    df = pd.DataFrame(analysis_result)
    if (len(df) > 0):
        col_mapping = {
            '6': 'day_1',
            '7': 'day_2',
            '8': 'day_3',
            '9': 'day_4',
            '10': 'day_5',
            '11': 'day_6',
            '12': 'day_7',
            '13': 'day_8',
            '14': 'day_9',
            '15': 'day_10'
        }
        for index, row in df.iteritems():
            if (str(index) in col_mapping):
                mean = row.apply(lambda item: float(item)).mean()
                median = row.apply(lambda item: float(item)).median()
                std = row.apply(lambda item: float(item)).std()
                max = row.apply(lambda item: float(item)).max()
                max_ts_code = df[df[index] == str(max).ljust(10, '0')][3].iloc[0]
                min = row.apply(lambda item: float(item)).min()
                min_ts_code = df[df[index] == str(min).ljust(10, '0')][3].iloc[0]
                print('Correlation for ' + col_mapping[str(index)])
                print('Mean: ' + str(mean))
                print('Median: ' + str(median))
                print('Std: ' + str(std))
                print('Max: ' + str(max))
                print('Max ts_code: ' + str(max_ts_code))
                print('Min: ' + str(min))
                print('Min ts_code: ' + str(min_ts_code))
                row1 = row.apply(lambda item: float(item) + 1)
                # draw_histogram(row1, 50)
                print('--------------------------------------')
 
'''
    因子相关性分析
'''
def factor_correlation_analysis(factor1, factor2, stock_list = [], save_result = True):
    persistence = DaoMysqlImpl()
    if (len(stock_list) == 0):
        stock_list = persistence.select("select ts_code from static_stock_list")
    else:
        save_result = False
    for stock in stock_list:
        print("Handle stock: " + stock[0])
        data = FileUtils.get_file_by_ts_code(stock[0], True)
        if (_filter_stock([NewStockFilter([30])], data)):
            correlation = '{:.8f}'.format(factor1.compare(factor2, data)[0])
            print(correlation)
            if (save_result):
                item = (factor1.get_factor_code(), factor1.get_params()[0], factor2.get_factor_code(), factor2.get_params()[0], stock[0], correlation, tools.get_current_date())
                persistence.insert('insert into factor_correlation_result values (REPLACE(UUID(),"-",""),%s,%s,%s,%s,%s,%s,%s)', [item])
                   
#生成统计信息
def create_stock_statistics(trade_date):
    persistence = DaoMysqlImpl()
    stock_list = persistence.select("select ts_code from static_stock_list")
    rising_count = 0
    falling_count = 0
    flat_count = 0
    for stock in stock_list:
        data = FileUtils.get_file_by_ts_code(stock[0], True)
        data['delta'] = data['close'] - data['close'].shift(1)
        if (data[data['trade_date'] == trade_date]['delta'].max() > 0):
            rising_count = rising_count + 1
        elif (data[data['trade_date'] == trade_date]['delta'].max() < 0):
            falling_count = falling_count + 1
        else:
            flat_count = flat_count + 1
    persistence.delete('delete from stock_statistics where trade_date = ' + trade_date)
    item = (trade_date, str(rising_count), str(falling_count), str(flat_count))
    persistence.insert('insert into stock_statistics values (%s, %s, %s, %s)', [item])
        



if __name__ == '__main__':
    # print(_ts_code_transform('100000.SZ'))
    
    # factor = LowerHatch([5])
    # factor = MeanInflectionPoint([10])
    # factor1 = MeanTrend([20])
    # factor = RisingTrend([10])
    # factor2 = MeanPenetration([20])
    # factor = EnvelopePenetration_Keltner([20])
    # factor = EnvelopePenetration_Keltner([20])
    # factor = AdvanceEnvelopePenetration_Keltner([20])
    # factor = KDJRegression([9])
    # factor = RSIPenetration([14])
    # factor = DRFPenetration([0.3])
    # factor = WRRegression([30])
    # factor = UOPenetration([7,14,28])
    # factor = FIPenetration([13])
    # factor = MFIPenetration([14])
    # factor_list = []
    # factor_list.append(MeanPenetration([20]))
    # factor_list.append(MeanTrend([20]))
    # training_model = TrainingModel(1)
    # learn = MachineLearn(factor_list, training_model)
    # factor = CompoundFactor(learn)
    # select_stock(factor)
    # print(data)
    # print(retro_select_stock(factor, '20211214'))
    # 单一股票相关性分析
    # data = FileUtils.get_file_by_ts_code('605089.SH', True)
    # correlation_analysis(factor, data, save_result = False)
    # 生成全部股票相关性分析结果
    # correlation_analysis_all_stocks(factor)
    # 相关性分析结果统计
    # correlation_analysis_report(factor)
    # 因子相关性分析
    # factor_correlation_analysis(factor1, factor2)
    # 打开选股结果
    open_selected_stocks_link('4e47cdd6-2304-11ed-8de4-acde4800')
    # 声称股票统计信息
    # create_stock_statistics('20220321')
    