#! /usr/bin/env python
# -*- coding:utf8 -*-

"""
这个文件主要用来做一些金融领域的基本功能
1. 生成k线
"""

import pandas as pd

from visualization import draw_analysis_curve
from persistence import DaoMysqlImpl
from tools import get_time_base, add_minutes, format_time

#生成k线
def create_k_line(instrument, unit = 1, directly_from_db = False):
    persistence = DaoMysqlImpl()
    kdata = persistence.get_future_kline_data(instrument)
    data = persistence.get_future_tick_data(instrument)
    start_time = data['tick'].min()
    latest_time = data['tick'].max()
    kline_dt_columns = ['time','open','close','low','high','volume','open_interest','product','instrument']
    data_df = pd.DataFrame(columns=kline_dt_columns)
    if (len(kdata) > 0):
        #如果已经生成了k线数据，从已经生成的k线数据继续向后计算
        start_time = kdata['time'].max()
        data = persistence.get_future_tick_data(instrument, format_time(start_time))
    if (len(data) > 0):
        need_init = True
        total_count = len(data)
        while(total_count > 0):
            if (need_init):
                start_time = get_time_base(start_time, unit)
                end_time = add_minutes(start_time, unit)
                need_init = False
            else:
                start_time = add_minutes(start_time, unit)
                end_time = add_minutes(start_time, unit)
            temp_data = data.loc[(data['tick'] >= start_time ) & (data['tick'] < end_time)]
            total_count = total_count - len(temp_data)
            # if (len(temp_data) >= 115):
            # 区间的结束时间如果比最新的tick时间小，说明可以加载这个区间
            if (end_time <= latest_time and len(temp_data) > 0):
                first_time = temp_data['tick'].min()
                last_time = temp_data['tick'].max()
                open = temp_data[temp_data['tick'] == first_time]['last_price'].iloc[0]
                close = temp_data[temp_data['tick'] == last_time]['last_price'].iloc[0]
                low = temp_data['last_price'].min()
                high = temp_data['last_price'].max()
                volume = temp_data[temp_data['tick'] == last_time]['trade_volume'].iloc[0]
                open_interest = temp_data[temp_data['tick'] == first_time]['open_interest'].iloc[0]
                product = instrument[0:2]
                item = [[end_time, open, close, low, high, volume, open_interest, product, instrument]]
                df = pd.DataFrame(item, columns=kline_dt_columns)
                data_df = data_df.append(df)
            data_df.index = data_df['time']
    if (directly_from_db):
        data_df = kdata
    return data_df

    
if __name__ == '__main__':
    data = create_k_line('RB2210')
    draw_analysis_curve(data, volume = False)
    print('aa')
    
        