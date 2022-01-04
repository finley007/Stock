#! /usr/bin/env python
# -*- coding:utf8 -*-

import datetime
import time
import webbrowser

def get_current_date():
    return datetime.datetime.now().strftime('%Y%m%d')

def get_current_time():
    return time.strftime("%H:%M:%S", time.localtime())

def get_date_scope(start_date, end_date):
    start_date_time = datetime.datetime.strptime(start_date,'%Y%m%d')
    end_date_time = datetime.datetime.strptime(end_date,'%Y%m%d')
    return (end_date_time - start_date_time).days

def create_instance(module_name, class_name, *args, **kwargs):
    module_meta = __import__(module_name, globals(), locals(), [class_name])
    class_meta = getattr(module_meta, class_name)
    obj = class_meta(*args, **kwargs)
    return obj

def run_with_timecost(func):
    def fun(*args, **kwargs):
        t = time.perf_counter()
        result = func(*args, **kwargs)
        print(f'Cost time: {time.perf_counter() - t:.8f} s')
        return result
    return fun

def to_number(str):
    try:
        return int(str)
    except ValueError:
        return float(str)
        

@run_with_timecost
def f1():
    time.sleep(1)
    
def open_url(url):
    webbrowser.open(url)
    
if __name__ == '__main__':
    # print(get_current_date())
    # print(get_date_scope('20210920','20210926'))
    # print(create_instance('sklearn.linear_model', 'LinearRegression'))
    # print(get_current_time() > '00:00:00' and get_current_time() < '15:00:00')
    # f1()
    open_url('https://finance.sina.com.cn/realstock/company/sh603611/nc.shtml')