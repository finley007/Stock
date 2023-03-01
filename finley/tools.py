#! /usr/bin/env python
# -*- coding:utf8 -*-

from datetime import datetime, timedelta
import time
import webbrowser
import inspect
import constants

"""
时间日期相关函数
"""
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

def date_format_convert(date, fmt1, fmt2):
    return datetime.strptime(date, fmt1).strftime(fmt2)

def get_time_base(time, unit):
    if unit == 1: #去掉秒和毫秒
        return time.replace(second = 0, microsecond = 0)
    return time

def get_next_n_day(current_date, n):
    current_date = datetime.strptime(current_date, "%Y%m%d")
    current_date = current_date + timedelta(days=n)
    return current_date.strftime("%Y%m%d")

def split_by_dash(str):
    return str.split("_")

def get_transaction_time(date):
    time_range = constants.TRANSACTION_TIME.replace('xxx', date).split('｜') 
    return list(map(split_by_dash, time_range))
            
def format_time(time):
    return time.strftime('%Y-%m-%d %H:%M:%S')

def add_minutes(time, mins):
    date_str = parse_date_from_time(time)
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

def parse_date_from_time(time):
    return datetime.strftime(time, '%Y-%m-%d')

def parse_time_from_time(time):
    return datetime.strftime(time, '%H:%M:%S')

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
    print(get_current_date())
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
    # print(date_format_convert('2022-05-08','%Y-%m-%d','%Y%m%d'))
    # print(parse_time_from_time('2021-01-08 09:33:00'))