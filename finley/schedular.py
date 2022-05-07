#! /usr/bin/env python
# -*- coding:utf8 -*-

from apscheduler.schedulers.background import BackgroundScheduler
import time
import uuid

from persistence import DaoMysqlImpl
from financetools import create_k_line
from futuretools import get_data_by_product_and_instrument
from factor.trend_factor import MeanInflectionPoint
import constants

executed_action_time = set()

#生成k线
def create_kline_job():
    # print('create k-line')
    persistence = DaoMysqlImpl()
    data = create_k_line('RB2210')
    item_list = data.apply(lambda x: tuple(x), axis = 1).values.tolist()
    persistence.insert('insert into 1_min_k_line values (%s,%s,%s,%s,%s,%s,%s,%s,%s)', item_list)
    # print('create k-line done')

#执行交易    
def execute_transaction_job():
     #不存在，则计算因子值决定是否开仓
    factor = MeanInflectionPoint([10])
    data = get_data_by_product_and_instrument('RB', 'RB2210', False)
    persistence = DaoMysqlImpl()
    global executed_action_time 
    #每隔10s执行一次
    #获取未平仓的交易记录，存在则根据最新价格判断是否更新止损价格或是平仓
    open_transaction = persistence.get_latest_opened_transaction('RB2210')
    if (len(open_transaction) > 0):
        if (open_transaction['type'][0] == constants.TRADE_TYPE_LONG):
            tick = persistence.get_latest_price('RB2210')
            if (tick['last_price'][0] <= open_transaction['stop_price'][0]):
                #平仓止损
                print("多平止损")
                persistence.update_close_action(open_transaction['id'][0], tick['last_price'][0], tick['tick'][0])
            data = factor.caculate(data)
            data.loc[:,'action'] = data.apply(lambda item:factor.get_action_mapping(item),axis=1)
            if (data.iloc[-factor.get_signal_delay()]['action'] == constants.CLOSE_TRANSACTION_ACTION and 
                data.iloc[-factor.get_signal_delay()]['time'] not in executed_action_time):
                print("多平")
                persistence.update_close_action(open_transaction['id'][0], tick['last_price'][0], tick['tick'][0])
                executed_action_time.add(data.iloc[-factor.get_signal_delay()]['time'])
            if (tick['last_price'][0] * stop_loss_rate > open_transaction['stop_price'][0]):
                print("更新多平止损线")
                persistence.update_stop_price('RB2210', tick['last_price'][0] * stop_loss_rate)
        else:
            tick = persistence.get_latest_price('RB2210')
            if (tick['last_price'][0] >= open_transaction['stop_price'][0]):
                #平仓止损
                print("空平止损")
                persistence.update_close_action(open_transaction['id'][0], tick['last_price'][0], tick['tick'][0])
            data = factor.caculate(data)
            data.loc[:,'action'] = data.apply(lambda item:factor.get_action_mapping(item),axis=1)
            if (data.iloc[-factor.get_signal_delay()]['action'] == constants.OPEN_TRANSACTION_ACTION and 
                data.iloc[-factor.get_signal_delay()]['time'] not in executed_action_time):
                print("空平")
                persistence.update_close_action(open_transaction['id'][0], tick['last_price'][0], tick['tick'][0])
                executed_action_time.add(data.iloc[-factor.get_signal_delay()]['time'])
            if (tick['last_price'][0] + tick['last_price'][0] * (1 - stop_loss_rate) < open_transaction[0]['stop_price']):
                print("更新空平止损线")
                persistence.update_stop_price('RB2210', tick['last_price'][0] + tick['last_price'][0] * (1 - stop_loss_rate))
    else:
        data = factor.caculate(data)
        data.loc[:,'action'] = data.apply(lambda item:factor.get_action_mapping(item),axis=1)
        if (data.iloc[-factor.get_signal_delay()]['action'] == constants.OPEN_TRANSACTION_ACTION and 
            data.iloc[-factor.get_signal_delay()]['time'] not in executed_action_time):
            #多开
            print("多开")
            tick = persistence.get_latest_price('RB2210')
            item = (uuid.uuid1(), 'RB2210', 100, constants.TRADE_TYPE_LONG, '0' ,tick['last_price'][0], tick['tick'][0], 0, '', tick['last_price'][0] * stop_loss_rate, factor.get_factor_code(), 0, factor.get_params()[0], 0, 0)
            persistence.insert('insert into transaction_record values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [item])
            executed_action_time.add(data.iloc[-factor.get_signal_delay()]['time'])
        elif (data.iloc[-factor.get_signal_delay()]['action'] == constants.CLOSE_TRANSACTION_ACTION and 
              data.iloc[-factor.get_signal_delay()]['time'] not in executed_action_time):
            #空开
            print("空开")
            tick = persistence.get_latest_price('RB2210')
            item = (uuid.uuid1(), 'RB2210', 100, constants.TRADE_TYPE_SHORT, '0',tick['last_price'][0], tick['tick'][0] , 0, '', tick['last_price'][0] + tick['last_price'][0] * (1 - stop_loss_rate), factor.get_factor_code(), 0, factor.get_params()[0], 0, 0)
            persistence.insert('insert into transaction_record values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [item])
            executed_action_time.add(data.iloc[-factor.get_signal_delay()]['time'])
    

if __name__=='__main__':

    stop_loss_rate = 0.95
    # create_kline_job()
    # execute_transaction_job()
    sched = BackgroundScheduler(timezone='MST')
    sched.add_job(create_kline_job, 'interval', id='create_kline_job', minutes=1.1)
    sched.add_job(execute_transaction_job, 'interval', id='execute_transaction_job', seconds=10)

    # #模拟插入实时数据
    persistence = DaoMysqlImpl()
    # persistence.delete('delete from real_time_tick')
    tick_list = persistence.select("select * from real_time_tick_bak where tick > (select max(tick) from real_time_tick) order by tick")
    # tick_list = persistence.select("select * from real_time_tick_bak order by tick")
    sched.start()
    for tick in tick_list:
        print('insert tick: ' + str(tick[0]))
        persistence.insert('insert into real_time_tick values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [tick])
        time.sleep(0.5)
        print('insert tick done')
    # time.sleep(1000000)
    