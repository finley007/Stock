#! /usr/bin/env python
# -*- coding:utf8 -*-

import cmd
import uuid

from persistence import DaoMysqlImpl

'''
命令行录入信息基类
'''
class Operation(cmd.Cmd):

    prompt = '>'

    def do_exit(self, _):
        '退出'
        exit(0)

'''
股票交易操作
'''        
class Trade(Operation):
    
    intro = '股票交易，输入help或者?查看帮助。\n'
    
    def do_buy(self, args):
        '交易股票，参数: analysis_id volume price date'
        self.buy(args)
        
    def buy(self, arg):
        args = arg.split(' ')
        if (len(args) < 5):
            print('交易股票，参数: ts_code factor_code param_value volume price date')
        else:
            trade_id = uuid.uuid1()
            ts_code = args[0]
            factor_code = args[1]
            param_value = args[2]
            volume = int(args[3])
            price = float(args[4])
            date = args[5]
            persistence = DaoMysqlImpl()
            item = (trade_id, ts_code, volume, price, date, 0, '', factor_code, 0, param_value, 0, 0)
            persistence.insert('insert into transaction_record values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [item])
        


if __name__ == '__main__':
   Trade().cmdloop()