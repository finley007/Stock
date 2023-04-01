#! /usr/bin/env python
# -*- coding:utf8 -*-

"""
这个文件主要用来做股票数据的处理
"""

def get_rising_falling_limit(stock, date):
    """
    自20200824日起，科创办68xxxx和创业板300xxx股票涨跌幅调整为20%
    Parameters
    ----------
    stock
    date

    Returns
    -------

    """
    if stock[0:2] == '68' or stock[0:3] == '300':
        if date >= '20200824':
            return 0.2
    return 0.1

    
if __name__ == '__main__':
    print(get_rising_falling_limit('300451SZ', '20220102'))
    
        