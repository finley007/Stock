#! /usr/bin/env python
# -*- coding:utf8 -*-
from configparser import ConfigParser

class Config: 
    cp = ConfigParser()
    
    def __init__(self):
        self.cp.read('system.ini')
        
    def get(self, section, key):
        return self.cp.get(section, key)
    
if __name__ == '__main__':
    config = Config()
    print(config.get('sina','stock_info_link_template'))
    