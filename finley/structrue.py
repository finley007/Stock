#! /usr/bin/env python
# -*- coding:utf8 -*-

class Sample(object):
    
    def __init__(self, ts_code, start_date = '', end_date = ''):
        self.ts_code = ts_code
        self.start_date = start_date
        self.end_date = end_date
        
    def get_ts_code(self):
        return self.ts_code
    
    def get_start_date(self):
        return self.start_date
    
    def get_end_date(self):
        return self.end_date