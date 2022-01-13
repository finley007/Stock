#! /usr/bin/env python
# -*- coding:utf8 -*-

import tools
import uuid

from persistence import DaoMysqlImpl

class Action(object):
    
    _stop_loss_rate = 0.9
    
    _open_date = ''
    _close_date = ''
    # 增加止损线
    _stop_loss_date = ''
    _open_price = 0
    _close_price = 0
    _stop_loss_price = 0
    
    def __init__(self, open_date, open_price, data):
        self._open_date = open_date
        self._open_price = open_price.iloc[0]
        self._stop_loss_price = self._open_price * self._stop_loss_rate
        filter_data = data.loc[(data['trade_date'] > self._open_date) & (data['low'] <= self._stop_loss_price)].sort_values(by='trade_date')
        if (len(filter_data) > 0):
            self._stop_loss_date = filter_data.iloc[0]['trade_date']
        
    def set_close_date(self, close_date, data):
        self._close_date = close_date
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
                    if (row['high'] < self._stop_loss_price):
                        self._stop_loss_price = row['high']
                    self._stop_loss_date = row['trade_date']
                    break
                else:
                    if ((row['high'] * self._stop_loss_rate) > self._stop_loss_price):
                        self._stop_loss_price = row['high'] * self._stop_loss_rate
        
    def set_close_price(self, close_price):
        self._close_price = close_price.iloc[0]
        
    def get_open_date(self):
        return self._open_date
        
    def get_close_date(self):
        if (self._stop_loss_date != '' and self._stop_loss_date <= self._close_date):
            return self._stop_loss_date
        else:    
            return self._close_date
    
    def get_open_price(self):
        return self._open_price
        
    def get_close_price(self):
        if (self._stop_loss_date != '' and self._stop_loss_date <= self._close_date):
            return self._stop_loss_price
        else:
            return self._close_price
        
    def get_profit_rate(self):
        return (self.get_close_price() - self.get_open_price())/self.get_open_price()

def simulate(factor, data, start_date = '', end_date = '', save = True):
        if (len(data) <= 30):
            print("Sub-new stock, will be ignore for simulation")
            return
        dao = DaoMysqlImpl()
        data = factor.caculate(data)
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
                current_action = Action(trade_date, data[data['trade_date'] == trade_date]['open'], data) 
            for action in sell_action_list.iterrows():
                factor_date = action[1]['trade_date']
                #招开仓日子后的第一个平仓信号
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
            if (current_action.get_close_date() == '' and not data[data['trade_date'] == end_date].empty):
                current_action.set_close_date(end_date, data)
                current_action.set_close_price(data[data['trade_date'] == end_date]['low'])
            if (current_action.get_close_date() != ''):
                action_records.append(current_action)
        print_simulate_result(factor, data, action_records, start_date, end_date, factor.get_version(), save)
        
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