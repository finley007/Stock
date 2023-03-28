#! /usr/bin/env python
# -*- coding:utf8 -*-

from abc import ABCMeta, abstractclassmethod
from sqlalchemy import Column, String, Integer, DateTime, Date, Time, BigInteger, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
import datetime
import pandas as pd
import connectorx as cx
import pymysql
import gzip
import _pickle as cPickle
import os
import shutil
from glob import glob
import uuid
from functools import lru_cache

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
    def get_file_by_product_and_instrument(product, instrument, add_product = False):
        data = pd.read_pickle(constants.FUTURE_DATA_PATH + product + '/' + instrument + '-1m.pkl')
        if (add_product):
            data['product'] = product
            data['instrument'] = instrument
        return data
    
    # 获取指定目录下的所有文件
    @staticmethod
    def read_files_in_path(path):
        all_files = list(map(lambda x: x, list(set(os.listdir(path)) - set(constants.EXCLUDED_FILES))))
        return all_files
    
    # 复制文件
    @staticmethod
    def copy_file(source_file, target_file):
        shutil.copy(source_file, target_file)
        
    # 模糊搜索文件
    def search_file(pattern, search_path, pathsep=os.pathsep):
        for path in search_path.split(os.pathsep):
            for match in glob(os.path.join(path, pattern)):
                yield match
            

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
    def get_next_n_business_date(self, current_date, n):
        pass
    
    @abstractclassmethod
    def get_learning_model(self, model_id):
        pass
    
    @abstractclassmethod
    def get_future_tick_data(self, instrument, start_date = '', end_date = ''):
        pass
    
    @abstractclassmethod
    def get_future_kline_data(self, instrument, start_date = '', end_date = ''):
        pass
    
class DaoMysqlImpl(Dao):   
    
    _db_url = '{}://{}:{}@{}:{}/{}'.format(constants.DB_TYPE, constants.DB_USERNAME, constants.DB_PASSWORD, constants.DB_HOST, constants.DB_PORT, constants.DB_NAME) 
    
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

    @lru_cache(maxsize=1000)
    def get_next_n_business_date(self, current_date, n):
        result = self.select('select cal_date from static_calendar where cal_date > ' + current_date + ' and is_open = 1 order by cal_date')
        if len(result) >= n: 
            return result[n-1][0]
        else:
            return ''
    
    def get_learning_model(self, model_id):
        result = self.select('select model from learning_model where id = ' + str(model_id))
        return json.loads(result[0][0])
    
    def get_future_tick_data(self, instrument, start_date = '', end_date = ''):
        sql = "select * from real_time_tick where instrument_id = '" + instrument + "'"
        if (start_date != ''):
            sql = sql + " and tick >= '" + start_date + "'"
        if (end_date != ''):
            sql = sql + " and tick <= '" + end_date + "'"
        sql = sql + " order by tick"
        data = cx.read_sql(self._db_url, sql)
        return data
    
    def get_future_kline_data(self, instrument, unit = 1, start_date = '', end_date = ''):
        sql = "select * from 1_min_k_line where instrument = '" + instrument + "'"
        if (start_date != ''):
            sql = sql + " and time >= '" + start_date + "'"
        if (end_date != ''):
            sql = sql + " and time <= '" + end_date + "'"
        sql = sql + " order by time"
        data = cx.read_sql(self._db_url, sql)
        data.index = data['time']
        return data
    
    #获取最近的未平仓记录
    def get_latest_opened_transaction(self, instrument):
        sql = "select * from transaction_record where ts_code = '" + instrument + "'"
        sql = sql + " and status = '0'"
        data = cx.read_sql(self._db_url, sql)
        return data
    
    #获取最新价格
    def get_latest_price(self, instrument):
        sql = "select * from real_time_tick where instrument_id = '" + instrument + "'"
        sql = sql + " order by tick desc limit 1"
        data = cx.read_sql(self._db_url, sql)
        return data
    
    #更新止损价格
    def update_stop_price(self, instrument, stop_price):
        sql = "update transaction_record set stop_price = '" + str(stop_price) + "' where ts_code = '" + instrument + "' and status = '0'"
        self.update(sql)
        
    #更新平仓记录
    def update_close_action(self, id, close_price, close_date):
        sql = "update transaction_record set close_price = '" + str(close_price) + "', close_date = '" + str(close_date) + "', status = '1' where id = '" + id + "'"
        self.update(sql)
    
    #获取股票数据    
    def get_stock_list(self):
        stock_list = self.select("select ts_code from static_stock_list")
        return list(map(lambda stock: stock[0], stock_list))
 
def create_session():
    """创建一个数据库链接会话
    """
    pymysql.install_as_MySQLdb()
    from sqlalchemy import create_engine
    engine = create_engine(constants.DB_CONNECTION)
    from sqlalchemy.orm import sessionmaker
    DbSession = sessionmaker(bind=engine)
    session = DbSession()
    return session

Base = declarative_base()
   
class Test(Base):
    """测试表po：
    """
    __tablename__ = "test"

    varchar_column = Column(String(10), primary_key=True)
    int_column = Column(Integer)
    date_column = Column(Date)
    datetime_column = Column(DateTime)
    time_column = Column(Time)
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, varchar_column, int_column, date_column, datetime_column, time_column):
        self.varchar_column = varchar_column
        self.int_column = int_column
        self.date_column = date_column
        self.datetime_column = datetime_column
        self.time_column = time_column
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()
        
class FactorCase(Base):
    """因子场景表po：
    """
    __tablename__ = "factor_case"

    id = Column(String(128), primary_key=True)
    factor = Column(String(64))
    package = Column(String(64))
    version = Column(String(10))
    param = Column(String(50))
    threshold = Column(String(50))
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, package, factor, version, param, threshold):
        self.id = '_'.join([factor, version, param, threshold])
        self.package = package
        self.factor = factor
        self.version = version
        self.param = param
        self.threshold = threshold
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()


class DistributionResult(Base):
    """分布表po：
    """
    __tablename__ = "distribution_result"

    id = Column(String(32), primary_key=True)
    type = Column(Integer)
    related_id = Column(String(10))
    max = Column(DECIMAL(15, 5))
    min = Column(DECIMAL(15, 5))
    scope = Column(DECIMAL(15, 5))
    mean = Column(DECIMAL(15, 5))
    median = Column(DECIMAL(15, 5))
    std = Column(DECIMAL(15, 5))
    var = Column(DECIMAL(15, 5))
    ptile10 = Column(DECIMAL(15, 5))
    ptile20 = Column(DECIMAL(15, 5))
    ptile30 = Column(DECIMAL(15, 5))
    ptile40 = Column(DECIMAL(15, 5))
    ptile50 = Column(DECIMAL(15, 5))
    ptile60 = Column(DECIMAL(15, 5))
    ptile70 = Column(DECIMAL(15, 5))
    ptile80 = Column(DECIMAL(15, 5))
    ptile90 = Column(DECIMAL(15, 5))
    file_path = Column(String(128))
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, type, related_id, info, file_path):
        self.id = str(uuid.uuid4()).replace('-', '')
        self.type = type
        self.related_id = related_id
        self.max = info['max']
        self.min = info['min']
        self.scope = info['scope']
        self.mean = info['mean']
        self.median = info['median']
        self.std = info['std']
        self.var = info['var']
        self.ptile10 = info['ptile10']
        self.ptile20 = info['ptile20']
        self.ptile30 = info['ptile30']
        self.ptile40 = info['ptile40']
        self.ptile50 = info['ptile50']
        self.ptile60 = info['ptile60']
        self.ptile70 = info['ptile70']
        self.ptile80 = info['ptile80']
        self.ptile90 = info['ptile90']
        self.file_path = file_path
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()

                
class FactorAnalysis(Base):
    """因子分析表po：
    """
    __tablename__ = "factor_analysis"

    id = Column(String(32), primary_key=True)
    factor_case = Column(String(128))
    filters = Column(String(128))
    param_value = Column(String(10))
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, factor_case, filters, param_value):
        self.id = str(uuid.uuid4()).replace('-', '')
        self.factor_case = factor_case
        self.filters = filters
        self.param_value = param_value
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()
    
    def get_id(self):
        return self.id

class FactorRetDistribution(Base):
    """因子收益分布表po：
    """
    __tablename__ = "factor_ret_distribution"

    id = Column(String(32), primary_key=True)
    factor_case = Column(String(128))
    filters = Column(String(128))
    param_value = Column(String(10))
    ret1 = Column(String(32))
    ret2 = Column(String(32))
    ret3 = Column(String(32))
    ret4 = Column(String(32))
    ret5 = Column(String(32))
    ret6 = Column(String(32))
    ret7 = Column(String(32))
    ret8 = Column(String(32))
    ret9 = Column(String(32))
    ret10 = Column(String(32))
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, factor_case, filters, rets, param_value):
        self.id = str(uuid.uuid4()).replace('-', '')
        self.factor_case = factor_case
        self.filters = filters
        self.param_value = param_value
        self.ret1 = rets[0]
        self.ret2 = rets[1]
        self.ret3 = rets[2]
        self.ret4 = rets[3]
        self.ret5 = rets[4]
        self.ret6 = rets[5]
        self.ret7 = rets[6]
        self.ret8 = rets[7]
        self.ret9 = rets[8]
        self.ret10 = rets[9]
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()
        
class SectionStockMapping(Base):
    """板块股票映射表po：
    """
    __tablename__ = "section_stock_mapping"

    section_code = Column(String(128), primary_key=True)
    ts_code = Column(String(10), primary_key=True)

    def __init__(self, section_code, ts_code):
        self.section_code = section_code
        self.ts_code = ts_code
        
class BaseDao():
    
    def __init__(self):
        self._session = create_session()


class FactorCaseDao(BaseDao):
    
        def get_factor_case_list_by_combination(self, combination_id):
            result_list = self._session.execute('select t3.*, t2.param from factor_combination t1, factor_combination_mapping t2, factor_case t3 where t1.id = t2.combination_id and t2.case_id = t3.id and t1.id = :combincation_id', {'combincation_id' : combination_id}).fetchall()
            return result_list
    
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
    # print(FileUtils.get_file_by_product_and_instrument('A', 'A1001'))
    # print(FileUtils.read_files_in_path(constants.FUTURE_DATA_PATH + 'IF'))
    # FileUtils.copy_file('/Users/finley/Projects/Stock/origin/AL/AL2205-1m.pkl', '/Users/finley/Projects/Stock/data/future/AL/AL2205-1m.pkl')
    
    # 拷贝
    # product_list = ['A','AG','AL','AP','AU','BU','C','CF','CS','CU','EB','EG','FG','FU','HC','I','IC','IF','IH','J','JD','JM','L','LU','M','MA','NI','OI','P','PG','PP','RB','RM','RU','SA','SC','SF','SM','SP','SR','T','TA','TF','V','Y','ZC','ZN']
    # for product in product_list:
    #     files = list(FileUtils.search_file('*-1m.pkl', '/Users/finley/Projects/Stock/origin/' + product + '/'))
    #     for file in files:
    #         target_files = list(FileUtils.search_file(file.split('/')[7], '/Users/finley/Projects/Stock/data/future/' + product + '/'))
    #         if (len(target_files) == 0):
    #             FileUtils.copy_file('/Users/finley/Projects/Stock/origin/' + product + '/' + file.split('/')[7], '/Users/finley/Projects/Stock/data/future/' + product + '/' + file.split('/')[7])
    
    dao = DaoMysqlImpl()
    # print(dao.get_future_data('rb2210', '2022-04-11 09:00:00', '2022-04-12 09:03:28'))
    # print(dao.get_stock_list())
    print(dao.get_next_n_business_date('20220203', 10))
    
    # po测试
    # session = create_session()

    # test_po = Test('test1', 40, '2022-12-18', '2022-12-18 02:12:13', '12:12:13')
    # session.add(test_po)
    # session.commit()
     
    # test_po = session.query(Test).filter(Test.varchar_column == 'test1').one()
    # test_po.int_column = 30
    # session.commit()

    # result = session.query(SectionStockMapping).filter(SectionStockMapping.section_code.in_(['BK0636','BK0428'])).all()
    # stock_list = list(map(lambda item : item.ts_code, result))
    # print(stock_list)
    
    # factor_case_dao = FactorCaseDao()
    # print(factor_case_dao.get_factor_case_list_by_combination('factor_combination1'))