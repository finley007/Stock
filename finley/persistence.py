#! /usr/bin/env python
# -*- coding:utf8 -*-

from abc import ABCMeta, abstractclassmethod

import pandas as pd
import pymysql
import gzip
import _pickle as cPickle

import constants
import json

from structrue import Sample
from tools import get_current_time, get_current_date

# 文件接口
class FileUtils(object):
    
    # 读取文件
    @staticmethod
    def load(path):
        with gzip.open(path, 'rb', compresslevel=1) as file_object:
            raw_data = file_object.read()
        return cPickle.loads(raw_data)

    # 保存文件
    @staticmethod
    def save(data, path):
        serialized = cPickle.dumps(data)
        with gzip.open(path, 'wb', compresslevel=1) as file_object:
            file_object.write(serialized)
            
    # 根据股票代码保存文件
    @staticmethod
    def save_file_by_ts_code(data, ts_code, is_reversion = False):
        if (is_reversion):
            return FileUtils.save(data, constants.STOCK_DATA_PATH + 'reversion/' + ts_code + '.pkl')
        else:
            return FileUtils.save(data, constants.STOCK_DATA_PATH + ts_code + '.pkl')
        
    # 根据股票代码获取文件
    @staticmethod
    def get_file_by_ts_code(ts_code, is_reversion = False):
        if (is_reversion):
            return FileUtils.load(constants.STOCK_DATA_PATH + '/reversion/' +  ts_code + '.pkl')
        else:
            return FileUtils.load(constants.STOCK_DATA_PATH + ts_code + '.pkl')
        
    # 根据产品和合约获取文件
    @staticmethod
    def get_file_by_product_and_instrument(product, instrument):
        return pd.read_pickle(constants.FUTURE_DATA_PATH + product + '/' + instrument + '-1m.pkl')
            

# 数据库接口
class Dao(metaclass = ABCMeta):
    
    @abstractclassmethod
    def insert(self, sql, params=[]):
        pass
    
    @abstractclassmethod
    def select(self, sql, params=[]):
        pass
    
    @abstractclassmethod
    def update(self, sql, params=[]):
        pass
    
    @abstractclassmethod
    def delete(self, sql, params=[]):
        pass
    
    @abstractclassmethod
    def get_last_business_date(self):
        pass
    
    @abstractclassmethod
    def get_next_business_date(self, current_date):
        pass
    
    @abstractclassmethod
    def get_learning_model(self, model_id):
        pass
    
class DaoMysqlImpl(Dao):   
    
    def __get_connection(self):
        conn = pymysql.connect(host = constants.DB_HOST, user = constants.DB_USERNAME, passwd = constants.DB_PASSWORD)
        conn.select_db(constants.DB_NAME)
        return conn

    def insert(self, sql, params=[]):
        conn = self.__get_connection()
        cur = conn.cursor()
        insert = cur.executemany(sql, params)
        cur.close()
        conn.commit()
        conn.close()
        return insert

    def select(self, sql, params=[]):
        conn = self.__get_connection()
        cur = conn.cursor()
        cur.execute(sql, params)
        result = cur.fetchall()
        cur.close()
        conn.close()
        return list(result)
    
    def update(self, sql, params=[]):
        conn = self.__get_connection()
        cur = conn.cursor()
        result = cur.execute(sql, params)
        conn.commit()
        cur.close()
        conn.close()
        return result
    
    def delete(self, sql, params=[]):
        conn = self.__get_connection()
        cur = conn.cursor()
        result = cur.execute(sql, params)
        conn.commit()
        cur.close()
        conn.close()
        return result
    
    def get_last_business_date(self):
        running_time = get_current_time()
        if (get_current_time() > '00:00:00' and get_current_time() < '15:00:00'):
            result = self.select('select max(cal_date) from static_calendar where cal_date < ' + get_current_date() + ' and is_open = 1')
        else:
            result = self.select('select max(cal_date) from static_calendar where cal_date <= ' + get_current_date() + ' and is_open = 1')
        return result[0][0]
    
    def get_next_business_date(self, current_date):
        result = self.select('select min(cal_date) from static_calendar where cal_date > ' + current_date + ' and is_open = 1')
        return result[0][0]
    
    def get_learning_model(self, model_id):
        result = self.select('select model from learning_model where id = ' + str(model_id))
        return json.loads(result[0][0])
    
if __name__ == '__main__':
    # dao = DaoMysqlImpl()
    # print(dao.select('select * from static_stock_list'))
    
    # df = pd.DataFrame({'source':['andriod','windows','iphone','linux','360浏览器']
    #                    ,'count':[45,12,80,45,24]})
    # print(df)
    # FileUtils.save(df, constants.TEMP_PATH + 'test.pkl')
    # df1 = FileUtils.load(constants.TEMP_PATH + 'test.pkl')
    # print(df1)
    # print(dao.get_last_business_date())
    # print(dao.get_next_business_date('20210925'))
    # print(dao.get_factor_case('MeanInflectionPoint_5_20210101_20210929'))
    data = FileUtils.get_file_by_product_and_instrument('A', 'A1001')
    print(data)