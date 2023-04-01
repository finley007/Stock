#! /usr/bin/env python
# -*- coding:utf8 -*-

from factor.base_factor import CombinedParamFactor
from persistence import FileUtils
from stocktools import get_rising_falling_limit
from tools import approximately_equal_to


class LimitUp(CombinedParamFactor):
    """
    涨停板
    """
    
    factor_code = 'limit_up'
    version = '1.0'
    
    def __init__(self, params):
        self._params = params
        
    def caculate(self, data, create_signal=True):
        data['increase'] = (data["close"] - data.shift(1)['close'])/data.shift(1)['close']
        data['limit_up'] = data.apply(lambda item: self.is_limit_up(item), axis=1)
        if create_signal:
            if self._params[0] == 1:
                # 当日涨停板
                data[self.get_signal()] = data['limit_up'].apply(lambda item: self.get_action_mapping(item))
            else:    
                data[self.get_signal()] = data['limit_up'].rolling(self._params[0]).apply(lambda item: self.get_action_mapping_with_param(self._params[0], item))
        return data
    
    def is_limit_up(self, item):
        limit_up = get_rising_falling_limit(item['ts_code'], item['trade_date'])
        return approximately_equal_to(limit_up, item['increase'])
    
    def get_action_mapping(self, item):
        if item:
            return 1
        else:
            return 0
        
    def get_action_mapping_with_param(self, param, item):
        limit_up = True
        for i in range(param):
            limit_up = limit_up and item[i]
        if (limit_up):
            return 1
        else:
            return 0
        

if __name__ == '__main__':
     data = FileUtils.get_file_by_ts_code('002757.SZ', is_reversion = True)
     factor = LimitUp([1])
     factor.caculate(data)
     print(data[factor.get_signal()])