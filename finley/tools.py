#! /usr/bin/env python
# -*- coding:utf8 -*-

from datetime import datetime, timedelta
import time
import webbrowser
import inspect
import constants

def get_current_date():
    return datetime.now().strftime('%Y%m%d')

def get_current_time():
    return time.strftime("%H:%M:%S", time.localtime())

def parse_time(time_str, with_millisecond = False):
    if (with_millisecond):
        return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f') 
    else:
        return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S') 

def date_to_time(date_str):
    time = datetime.strptime(date_str,'%Y%m%d')
    return format_time(time)

def get_time_base(time, unit):
    if unit == 1: #去掉秒和毫秒
        return time.replace(second = 0, microsecond = 0)
    return time

def split_by_dash(str):
    return str.split("_")

def get_transaction_time(date):
    time_range = constants.TRANSACTION_TIME.replace('xxx', date).split('｜') 
    return list(map(split_by_dash, time_range))
            
def format_time(time):
    return time.strftime('%Y-%m-%d %H:%M:%S')

# 仿真中获取得到交易信号之后实际的执行时间
def add_minutes_by_str(time_str, mins):
    time = parse_time(time_str)
    date_str = datetime.strftime(time, '%Y-%m-%d')
    time_range = get_transaction_time(date_str)
    new_time = format_time(time + timedelta(minutes=mins))
    if (new_time > time_range[0][1] and new_time < time_range[1][0]):
        return format_time(time + timedelta(minutes=(mins + 120)))
    elif (new_time > time_range[1][1] and new_time < time_range[2][0]):
        return format_time(time + timedelta(minutes=(mins + 360)))
    elif (new_time > time_range[2][1]):
        return format_time(time + timedelta(minutes=(mins + 600)))
    else:
        return new_time

def add_minutes(time, mins):
    date_str = datetime.strftime(time, '%Y-%m-%d')
    time_range = get_transaction_time(date_str)
    new_time = format_time(time + timedelta(minutes=mins))
    if (new_time > time_range[0][1] and new_time < time_range[1][0]):
        return time + timedelta(minutes=(mins + 120 - 1))
    elif (new_time > time_range[1][1] and new_time < time_range[2][0]):
        return time + timedelta(minutes=(mins + 360 - 1))
    elif (new_time > time_range[2][1]):
        return time + timedelta(minutes=(mins + 600 - 1))
    else:
        return time + timedelta(minutes=mins)

def get_date_scope(start_date, end_date):
    start_date_time = datetime.strptime(start_date,'%Y%m%d')
    end_date_time = datetime.strptime(end_date,'%Y%m%d')
    return (end_date_time - start_date_time).days

def create_instance(module_name, class_name, *args, **kwargs):
    module_meta = __import__(module_name, globals(), locals(), [class_name])
    class_meta = getattr(module_meta, class_name)
    obj = class_meta(*args, **kwargs)
    return obj

def get_all_class(module_name):
    class_list = []
    module = __import__(module_name)
    for package_name, package in inspect.getmembers(module, inspect.ismodule):
        for class_name, clz in inspect.getmembers(package, inspect.isclass):
            class_list.append(clz)
    return class_list

def run_with_timecost(func):
    def fun(*args, **kwargs):
        t = time.perf_counter()
        result = func(*args, **kwargs)
        print(f'Cost time: {time.perf_counter() - t:.8f} s')
        return result
    return fun

def to_params(str):
    if (str.find("|") != -1):
        params = list(map(lambda str: int(str), str.split("|")))
        return params
    try:
        return [int(str)]
    except ValueError:
        return [float(str)]

@run_with_timecost
def f1():
    time.sleep(1)
    
def open_url(url):
    webbrowser.open(url)
    
if __name__ == '__main__':
    # print(get_current_date())
    # print(get_current_time())
    # print(parse_time('2021-01-08 09:33:00'))
    # print(get_date_scope('20210920','20210926'))
    # print(create_instance('sklearn.linear_model', 'LinearRegression'))
    # print(get_current_time() > '00:00:00' and get_current_time() < '15:00:00')
    # f1()
    # open_url('https://finance.sina.com.cn/realstock/company/sh603611/nc.shtml')
    # print(to_params('25'))
    # print(to_params('25.45'))
    # print(to_params('25|24|23'))
    # get_all_class('factor.momentum_factor')
    # print(add_minutes('2021-01-08 09:33:00', 1))
    # print(date_to_time('20220101'))
    # time = parse_time('2022-04-12 09:03:27.500000', True)
    # print(get_time_base(time, 1))
    # print(get_transaction_time('2022-04-12'))
    print(add_minutes_by_str('2022-04-12 11:29:27', 5))
    print(add_minutes_by_str('2022-04-12 14:59:27', 10))