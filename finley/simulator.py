#! /usr/bin/env python
# -*- coding:utf8 -*-

import tools
import uuid
import ray
from abc import ABCMeta, abstractclassmethod

from persistence import DaoMysqlImpl, FileUtils
from visualization import draw_line
import constants


#仿真交易类
class Action(metaclass = ABCMeta):
    
    _stop_loss_rate = 0.9
    
    _open_date = ''
    _close_date = ''
    _open_price = 0
    _close_price = 0
     # 增加止损线
    _stop_loss_date = ''
    _stop_loss_price = 0
    
    def __init__(self, open_date, open_price, data):
        self._open_date = open_date
        self._open_price = open_price
        self.init_stop_loss_price() 
        #已开仓价计算止损日
        self.init_stop_loss_date(data)
        
    def set_close_date(self, close_date, data):
        self._close_date = close_date
        self.caculate_stop_loss_price_and_date(data)
        
    def set_close_price(self, close_price):
        self._close_price = close_price.iloc[0]
        
    def get_open_date(self):
        return self._open_date
    
    #提前止损    
    def get_close_date(self):
        if (self._stop_loss_date != '' and self._stop_loss_date <= self._close_date):
            return self._stop_loss_date
        else:    
            return self._close_date
    
    def get_open_price(self):
        return self._open_price
     
    #提前止损    
    def get_close_price(self):
        if (self._stop_loss_date != '' and self._stop_loss_date <= self._close_date):
            return self._stop_loss_price
        else:
            return self._close_price
        
    def get_profit_rate(self):
        return self.get_profit()/self.get_open_price()
    
    @abstractclassmethod
    def init_stop_loss_price(self):
        pass
    
    @abstractclassmethod
    def init_stop_loss_date(self, data):
        pass
    
    @abstractclassmethod
    def caculate_stop_loss_price_and_date(self, data):
        pass
    
    @abstractclassmethod
    def get_action_type(self):
        pass
    
    @abstractclassmethod
    def get_profit(self):
        pass

#做空   
class ShortAction(Action):
        
    def get_action_type(self):
        return "做空"
    
    def get_profit(self):
        charge_rate = float(constants.TRANSACTION_CHARGE_RATE)
        return self.get_open_price() * (1 - charge_rate) - self.get_close_price() * (1 + charge_rate)
    
    def init_stop_loss_price(self):
        self._stop_loss_price = self._open_price + self._open_price * (1 - self._stop_loss_rate)
        
    def init_stop_loss_date(self, data):
        filter_data = data.loc[(data['trade_date'] > self._open_date) 
                               & (data['high'] >= self._stop_loss_price)].sort_values(by='trade_date')
        if (len(filter_data) > 0):
            self._stop_loss_date = filter_data.iloc[0]['trade_date']
            
    def caculate_stop_loss_price_and_date(self, data):
        filter_data = data.loc[(data['trade_date'] >= self._open_date) & (data['trade_date'] <= self._close_date)].sort_values(by='trade_date')
        filter_data.reset_index(drop=True, inplace=True)
        #遍历开仓日期和原本的平仓日期之间的每一天
        for index, row in filter_data.iterrows():
            if (index == 0):
                if ((row['low'] / self._stop_loss_rate) < self._stop_loss_price):
                    self._stop_loss_price = row['low'] / self._stop_loss_rate
            else:
                #先处理止损，保守算法，因为最高价和最低价的先后顺序从k线图上已经无法获得，都先处理止损
                if (row['high'] > self._stop_loss_price):
                    # if (row['low'] > self._stop_loss_price):
                    self._stop_loss_price = row['high']
                    self._stop_loss_date = row['trade_date']
                    break
                else:
                    if ((row['low'] / self._stop_loss_rate) < self._stop_loss_price):
                        self._stop_loss_price = row['low'] / self._stop_loss_rate
    
#做多
class LongAction(Action):
    
    def get_action_type(self):
        return "做多"
    
    def get_profit(self):
        charge_rate = float(constants.TRANSACTION_CHARGE_RATE)
        return self.get_close_price() * (1 - charge_rate) - self.get_open_price() * (1 + charge_rate)
    
    def init_stop_loss_price(self):
        self._stop_loss_price = self._open_price * self._stop_loss_rate
        
    def init_stop_loss_date(self, data):
        filter_data = data.loc[(data['trade_date'] > self._open_date) 
                               & (data['low'] <= self._stop_loss_price)].sort_values(by='trade_date')
        if (len(filter_data) > 0):
            self._stop_loss_date = filter_data.iloc[0]['trade_date']
            
    def caculate_stop_loss_price_and_date(self, data):
        filter_data = data.loc[(data['trade_date'] >= self._open_date) & (data['trade_date'] <= self._close_date)].sort_values(by='trade_date')
        filter_data.reset_index(drop=True, inplace=True)
        #遍历开仓日期和原本的平仓日期之间的每一天
        for index, row in filter_data.iterrows():
            if (index == 0):
                if ((row['high'] * self._stop_loss_rate) > self._stop_loss_price):
                    self._stop_loss_price = row['high'] * self._stop_loss_rate
            else:
                #先处理止损，保守算法，因为最高价和最低价的先后顺序从k线图上已经无法获得，都先处理止损
                if (row['low'] < self._stop_loss_price):
                    # if (row['high'] < self._stop_loss_price):
                    self._stop_loss_price = row['low']
                    self._stop_loss_date = row['trade_date']
                    break
                else:
                    if ((row['high'] * self._stop_loss_rate) > self._stop_loss_price):
                        self._stop_loss_price = row['high'] * self._stop_loss_rate

'''
仿真器基类
'''
class Simulator(metaclass = ABCMeta):
    
    #仿真主方法
    def simulate(self, factor, data, start_date = '', end_date = '', save = True):
        #数据检查
        if (not self.validate(data)):
            return 
        #预处理
        data = self.pre_handle(data)
        #因子计算
        data = factor.caculate(data)
        #数据过滤
        filter_data = self.filter(data, start_date, end_date)
        data = filter_data[0]
        start_date = filter_data[1]
        end_date = filter_data[2]
        #仿真
        action_records = self.execute_simulate(data, factor, start_date, end_date)
        self.print_simulate_result(factor, data, action_records, start_date, end_date, factor.get_version(), save)
        return action_records
        
    #结果处理
    def print_simulate_result(self, factor, data, action_records, start_date, end_date, version, save = True):
        win_count = 0
        loss_count = 0
        profit = 0
        max_profit = 0
        max_profit_open_date = ''
        max_profit_open_price = ''
        max_profit_close_date = ''
        max_profit_close_price = ''
        min_profit = 0
        min_profit_open_date = ''
        min_profit_open_price = ''
        min_profit_close_date = ''
        min_profit_close_price = ''
        for action in action_records:
            print('+++++++++++++++++++++')
            print('交易类型:' + action.get_action_type())
            print(action.get_open_date() + '-' + action.get_close_date())
            print(str(action.get_open_price()) + '-' + str(action.get_close_price()))
            current_profit = action.get_profit()
            profit = profit + current_profit
            if (action.get_profit_rate() > 0):
                win_count = win_count + 1
            else:
                loss_count = loss_count + 1
            if (current_profit > max_profit):
                max_profit = current_profit
                max_profit_open_date = action.get_open_date()
                max_profit_open_price = action.get_open_price()
                max_profit_close_date = action.get_close_date()
                max_profit_close_price = action.get_close_price()
            if (current_profit < min_profit):
                min_profit = current_profit
                min_profit_open_date = action.get_open_date()
                min_profit_open_price= action.get_open_price()
                min_profit_close_date = action.get_close_date()
                min_profit_close_price = action.get_close_price()
        #股票代码
        # print('股票代码:' + data.iloc[-1]['ts_code'])
        #日期范围
        # print('日期区间:' + str(tools.get_date_scope(start_date, end_date)))
        #交易次数        
        print('交易次数:' + str(len(action_records)))
        #获利次数
        print('获利次数:' + str(win_count))
        #亏损次数
        print('亏损次数:' + str(loss_count))
        #最大获利
        print('最大获利:' + str(max_profit_open_date) + '|' + str(max_profit_close_date) + '|' + str(max_profit_open_price) + '|' + str(max_profit_close_price) + '|' + str(max_profit))
        #最大亏损
        print('最大亏损:' + str(min_profit_open_date) + '|' + str(min_profit_close_date) + '|' + str(min_profit_open_price) + '|' + str(min_profit_close_price) + '|' + str(min_profit))
        #利润
        print('利润:' + str(profit))
        #利润率
        print('利润率:' + str(profit/data.iloc[-1]['close']))
        if (save):
            factor_case = factor.get_factor_code() + '_' + str(factor.get_params()[0]) + '_' + start_date + '_' + end_date
            persistence = DaoMysqlImpl()
            item = (uuid.uuid1(), factor_case, self.get_ts_code(data), start_date, end_date, str(len(action_records)), str(win_count), str(loss_count), str(max_profit), str(min_profit), str(max_profit_open_date), str(min_profit_open_date), str(profit), str(profit/data.iloc[-1]['close']), version, self.get_type())
            persistence.insert('insert into simulation_result values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [item])
    
    #校验
    @abstractclassmethod
    def validate(self, data):
        pass
    
    #按时间过滤
    @abstractclassmethod
    def filter(self, data, start_date, end_date):
        pass
    
    #获取执行时间
    @abstractclassmethod
    def get_action_date(self, data, factor, factor_date):
        pass
    
    #预处理
    @abstractclassmethod
    def pre_handle(self, data):
        pass
    
    #执行仿真
    @abstractclassmethod
    def execute_simulate(self, data, factor, start_date, end_date):
        pass
    
    #获取代码
    @abstractclassmethod
    def get_ts_code(self, data):
        pass
    
    #获取仿真类型
    @abstractclassmethod
    def get_type(self):
        pass
 
#股票仿真器    
class StockSimulator(Simulator):
    
    def __init__(self):
        self._dao = DaoMysqlImpl()
    
    #校验
    def validate(self, data):
        if (len(data) <= 30):
            print("Sub-new stock, will be ignore for simulation")
            return False
        return True
    
    #按时间过滤
    def filter(self, data, start_date, end_date):
        if (end_date == ''):
            end_date = self._dao.get_last_business_date()
        if (start_date == ''):
            data = data[(data['trade_date'] <= end_date)]
            start_date = data['trade_date'].head(1).item()
        else:
            data = data[(data['trade_date'] >= start_date) & (data['trade_date'] <= end_date)]
        return (data, start_date, end_date)
    
    #获取执行时间
    def get_action_date(self, data, factor, factor_date):
        trade_date = self._dao.get_next_business_date(factor_date)
        signal_delay = factor.get_signal_delay()
        while(signal_delay > 1):
            trade_date = self._dao.get_next_business_date(trade_date)
            signal_delay = signal_delay - 1
        return trade_date
    
    #预处理
    def pre_handle(self, data):
        return data
    
    #执行仿真
    def execute_simulate(self, data, factor, start_date, end_date):
        data.loc[:,'action'] = data.apply(lambda item:factor.get_action_mapping(item),axis=1)
        buy_action_list = data[data['action'] == 1]
        sell_action_list = data[data['action'] == -1]
        current_action = None
        action_records = []
        for action in buy_action_list.iterrows():
            factor_date = action[1]['trade_date']
            # 如果开仓时间比当前平仓时间小则跳过
            if (current_action != None and factor_date <= current_action.get_close_date()):
                continue
            #当天出现交易信号第2天交易
            trade_date = self.get_action_date(data, factor, factor_date)
            if (not data[data['trade_date'] == trade_date].empty):
                current_action = LongAction(trade_date, data[data['trade_date'] == trade_date]['open'].iloc[0], data) 
            else:
                continue
            for action in sell_action_list.iterrows():
                factor_date = action[1]['trade_date']
                #找开仓日子后的第一个平仓信号
                if (factor_date <= current_action.get_open_date()):
                    continue
                trade_date = self.get_action_date(data, factor, factor_date)
                if (not data[data['trade_date'] == trade_date].empty):
                    current_action.set_close_date(trade_date, data)
                    current_action.set_close_price(data[data['trade_date'] == trade_date]['low'])
                    break
            if (current_action.get_close_date() == '' and not data[data['trade_date'] == end_date].empty):
                current_action.set_close_date(end_date, data)
                current_action.set_close_price(data[data['trade_date'] == end_date]['low'])
            if (current_action.get_close_date() != ''):
                action_records.append(current_action)
        return action_records
    
    def get_ts_code(self, data):
        return data.iloc[-1]['ts_code']
    
    def get_type(self):
        return 'STOCK'
    
    
#期货仿真器    
class FutrueSimulator(Simulator):
    
    def __init__(self):
        print("aa")
    
    #校验
    def validate(self, data):
        return True
    
    #按时间过滤
    def filter(self, data, start_date, end_date):
        if (end_date != ''):
            data = data[(data['trade_date'] <= end_date)]
        else:
            end_date = data['trade_date'].iloc[-1]
        if (start_date != ''):
            data = data[(data['trade_date'] >= start_date)]
        else:
            start_date = data['trade_date'].head(1).item()
        return (data, start_date, end_date)
    
    #获取执行时间
    def get_action_date(self, data, factor, factor_date):
        return tools.add_minutes_by_str(factor_date, int(factor.get_signal_delay()))
    
    #预处理
    def pre_handle(self, data):
        # 用index创建trade_date列
        data.index = tools.format_time(data.index)
        data['trade_date'] = data.index
        return data
    
    #执行仿真
    def execute_simulate(self, data, factor, start_date, end_date):
        data.loc[:,'action'] = data.apply(lambda item:factor.get_action_mapping(item),axis=1)
        action_list = data[data['action'] != 0]
        current_action = None
        action_records = [] #保留执行记录
        for action in action_list.iterrows():
            factor_date = action[1]['trade_date']
            trade_date = self.get_action_date(data, factor, factor_date)
            if (current_action == None):#开仓
                if (not data[data['trade_date'] == trade_date].empty):
                    if (action[1]['action'] == 1):
                        current_action = LongAction(trade_date, data[data['trade_date'] == trade_date]['open'].iloc[0], data)
                    else:
                        current_action = ShortAction(trade_date, data[data['trade_date'] == trade_date]['open'].iloc[0], data)
                else:
                    continue
            else:#平仓
                if (not data[data['trade_date'] == trade_date].empty and ((isinstance(current_action, LongAction) and action[1]['action'] == -1)
                    or (isinstance(current_action, ShortAction) and action[1]['action'] == 1))):
                    current_action.set_close_date(trade_date, data)
                    current_action.set_close_price(data[data['trade_date'] == trade_date]['low'])
                    action_records.append(current_action)
                    current_action = None
                else:
                    continue
        return action_records
    
    def get_ts_code(self, data):
        return data.iloc[-1]['instrument']
    
    def get_type(self):
        return 'FUTURE'
    
# @ray.remote
'''
仿真主方法
1. validation
2. filter
3. create action list
4. print result
'''
def simulate(factor, data, start_date = '', end_date = '', save = True):
        #数据检查
        if (len(data) <= 30):
            print("Sub-new stock, will be ignore for simulation")
            return
        data = factor.caculate(data)
        #获取开始和结束时间
        dao = DaoMysqlImpl()
        if (end_date == ''):
            end_date = dao.get_last_business_date()
        if (start_date == ''):
            data = data[(data['trade_date'] <= end_date)]
            start_date = data['trade_date'].head(1).item()
        else:
            data = data[(data['trade_date'] >= start_date) & (data['trade_date'] <= end_date)]
        data.loc[:,'action'] = data.apply(lambda item:factor.get_action_mapping(item),axis=1)
        buy_action_list = data[data['action'] == 1]
        sell_action_list = data[data['action'] == -1]
        current_action = None
        action_records = []
        for action in buy_action_list.iterrows():
            factor_date = action[1]['trade_date']
            # 如果开仓时间比当前平仓时间小则跳过
            if (current_action != None and factor_date <= current_action.get_close_date()):
                continue
            #当天出现交易信号第2天交易
            trade_date = dao.get_next_business_date(factor_date)
            signal_delay = factor.get_signal_delay()
            while(signal_delay > 1):
                trade_date = dao.get_next_business_date(trade_date)
                signal_delay = signal_delay - 1
            if (not data[data['trade_date'] == trade_date].empty):
                current_action = Action(trade_date, data[data['trade_date'] == trade_date]['open'].iloc[0], data) 
            else:
                continue
            for action in sell_action_list.iterrows():
                factor_date = action[1]['trade_date']
                #找开仓日子后的第一个平仓信号
                if (factor_date <= current_action.get_open_date()):
                    continue
                trade_date = dao.get_next_business_date(factor_date)
                signal_delay = factor.get_signal_delay()
                while(signal_delay > 1):
                    trade_date = dao.get_next_business_date(trade_date)
                    signal_delay = signal_delay - 1
                if (not data[data['trade_date'] == trade_date].empty):
                    current_action.set_close_date(trade_date, data)
                    current_action.set_close_price(data[data['trade_date'] == trade_date]['low'])
                    break
            if (current_action.get_close_date() == '' and not data[data['trade_date'] == end_date].empty):
                current_action.set_close_date(end_date, data)
                current_action.set_close_price(data[data['trade_date'] == end_date]['low'])
            if (current_action.get_close_date() != ''):
                action_records.append(current_action)
        print_simulate_result(factor, data, action_records, start_date, end_date, factor.get_version(), save)
        return action_records
        
def print_simulate_result(factor, data, action_records, start_date, end_date, version, save = True):
        win_count = 0
        loss_count = 0
        profit = 0
        max_profit = 0
        max_profit_open_date = ''
        max_profit_open_price = ''
        max_profit_close_date = ''
        max_profit_close_price = ''
        min_profit = 0
        min_profit_open_date = ''
        min_profit_open_price = ''
        min_profit_close_date = ''
        min_profit_close_price = ''
        for action in action_records:
            print('+++++++++++++++++++++')
            print(action.get_open_date() + '-' + action.get_close_date())
            print(str(action.get_open_price()) + '-' + str(action.get_close_price()))
            current_profit = action.get_close_price() - action.get_open_price()
            profit = profit + current_profit
            if (action.get_profit_rate() > 0):
                win_count = win_count + 1
            else:
                loss_count = loss_count + 1
            if (current_profit > max_profit):
                max_profit = current_profit
                max_profit_open_date = action.get_open_date()
                max_profit_open_price = action.get_open_price()
                max_profit_close_date = action.get_close_date()
                max_profit_close_price = action.get_close_price()
            if (current_profit < min_profit):
                min_profit = current_profit
                min_profit_open_date = action.get_open_date()
                min_profit_open_price= action.get_open_price()
                min_profit_close_date = action.get_close_date()
                min_profit_close_price = action.get_close_price()
        #股票代码
        print('股票代码:' + data.iloc[-1]['ts_code'])
        #日期范围
        print('日期区间:' + str(tools.get_date_scope(start_date, end_date)))
        #交易次数        
        print('交易次数:' + str(len(action_records)))
        #获利次数
        print('获利次数:' + str(win_count))
        #亏损次数
        print('亏损次数:' + str(loss_count))
        #最大获利
        print('最大获利:' + str(max_profit_open_date) + '|' + str(max_profit_close_date) + '|' + str(max_profit_open_price) + '|' + str(max_profit_close_price) + '|' + str(max_profit))
        #最大亏损
        print('最大亏损:' + str(min_profit_open_date) + '|' + str(min_profit_close_date) + '|' + str(min_profit_open_price) + '|' + str(min_profit_close_price) + '|' + str(min_profit))
        #利润
        print('利润:' + str(profit))
        #利润率
        print('利润率:' + str(profit/data.iloc[-1]['close']))
        if (save):
            factor_case = factor.get_factor_code() + '_' + str(factor.get_params()[0]) + '_' + start_date + '_' + end_date
            persistence = DaoMysqlImpl()
            item = (uuid.uuid1(), factor_case, data.iloc[-1]['ts_code'], start_date, end_date, str(len(action_records)), str(win_count), str(loss_count), str(max_profit), str(min_profit), str(max_profit_open_date), str(min_profit_open_date), str(profit), str(profit/data.iloc[-1]['close']), version)
            persistence.insert('insert into simulation_result values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [item])
            
            
def capital_curve_simulate(initial_capital_amount, trans_amout, factor, start_time, end_time='', products=[]):
    # 获取时间段内的所有合约
    instrument = get_instrument(start_time, end_time, products)
    charge_rate = float(constants.TRANSACTION_CHARGE_RATE)
    simulator = FutrueSimulator()
    data = FileUtils.get_file_by_product_and_instrument(instrument[0][0], instrument[0][1])
    data['assets'] = initial_capital_amount
    action_records = simulator.simulate(factor, data)
    for action in action_records:
        #做多
        if isinstance(action, LongAction):
            data.loc[(data.index >= action.get_open_date()) & (data.index <= action.get_close_date()), 'assets'] = data['assets'] - trans_amout * action.get_open_price() * (charge_rate + 1)  + trans_amout * data['close'] * (1 - charge_rate)
            data.loc[data.index > action.get_close_date(),'assets'] = data['assets'] - trans_amout * action.get_open_price() * (1 + charge_rate) + trans_amout * action.get_close_price() * (1 - charge_rate)
        #做空
        else:
            data.loc[(data.index >= action.get_open_date()) & (data.index <= action.get_close_date()), 'assets'] = data['assets'] + trans_amout * action.get_open_price() * (1 - charge_rate) - trans_amout * data['close'] * (1 + charge_rate)
            data.loc[data.index > action.get_close_date(),'assets'] = data['assets'] + trans_amout * action.get_open_price() * (1 - charge_rate) - trans_amout * action.get_close_price() * (1 + charge_rate)
    data['time'] = data.index
    draw_line(data,'Capital Curve','Time','Balance',{'x':'time','y':[{'key':'assets','label':'资金余额'}]})     
    print('aa')
    
def get_instrument(start_time, end_time='', products=[]):
    dao = DaoMysqlImpl()
    if (end_time == '' and len(products) == 0):
        return dao.select("select product, instrument from future_instrument_list where start_time > '" + start_time + "'")
    if (end_time == '' and len(products) > 0):
        return dao.select("select product, instrument from future_instrument_list where start_time > '" + start_time + "' and product in ({})".format(','.join(["'%s'" % product for product in products])))
    if (end_time != '' and len(products) > 0):
        return dao.select("select product, instrument from future_instrument_list where start_time > '" + start_time + "' and end_time < '" + end_time + "' and product in ({})".format(','.join(["'%s'" % product for product in products])))
    
if __name__ == '__main__':
    # 测试get_instrument
    print(get_instrument('2022-01-01 00:00:00'))
    print(get_instrument('2022-01-01 00:00:00', products = ['IC']))
    print(get_instrument('2022-01-01 00:00:00', '2022-03-20 00:00:00', products = ['IC']))
   
    
    
            