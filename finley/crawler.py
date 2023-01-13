#! /usr/bin/env python
# -*- coding:utf8 -*-

from abc import ABCMeta, abstractclassmethod
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium import webdriver
from persistence import DaoMysqlImpl, FileUtils
import time

#爬虫基类
class CrawlerTask(metaclass = ABCMeta):
    
    @abstractclassmethod
    def execute(self):
        pass
    
#板块信息爬虫
class SectionInfoTask(CrawlerTask):
    
    _init_url = "https://data.eastmoney.com/bkzj/{}.html"
    
    def execute(self):
        web = webdriver.Chrome(ChromeDriverManager( ).install())
        persistence = DaoMysqlImpl()
        section_list = persistence.select("select section_code, section_name from stock_section order by section_code")
        if (len(section_list) > 0):
            for section_code in section_list:
                latest_trade_date = persistence.select("select max(trade_date) as latest_trade_date from stock_section_statistics where section_code = '" + section_code[0] + "'")
                url = self._init_url.format(section_code[0])
                web.get(url)
                web.find_element(By.XPATH, '/html/body/div[1]/div[8]/div[2]/div[9]/div/ul/li[2]').click()
                hist_list = web.find_elements(By.XPATH, '//*[@id="table_ls"]/table/tbody/tr')
                for record in hist_list:
                    items = record.find_elements(By.TAG_NAME, 'td')
                    trade_date = items[0].text
                    if (latest_trade_date[0][0] == trade_date):
                        break
                    main_inflow = self.unified_unit(items[1].find_element(By.TAG_NAME, 'span').text)
                    main_inflow_rate = self.unified_unit(items[2].find_element(By.TAG_NAME, 'span').text)
                    super_large_inflow = self.unified_unit(items[3].find_element(By.TAG_NAME, 'span').text)
                    super_large_inflow_rate = self.unified_unit(items[4].find_element(By.TAG_NAME, 'span').text)
                    large_inflow = self.unified_unit(items[5].find_element(By.TAG_NAME, 'span').text)
                    large_inflow_rate = self.unified_unit(items[6].find_element(By.TAG_NAME, 'span').text)
                    middle_inflow = self.unified_unit(items[7].find_element(By.TAG_NAME, 'span').text)
                    middle_inflow_rate = self.unified_unit(items[8].find_element(By.TAG_NAME, 'span').text)
                    small_inflow = self.unified_unit(items[9].find_element(By.TAG_NAME, 'span').text)
                    small_inflow_rate = self.unified_unit(items[10].find_element(By.TAG_NAME, 'span').text)
                    item = (section_code[0], section_code[1], '', main_inflow, main_inflow_rate, super_large_inflow, super_large_inflow_rate,large_inflow,large_inflow_rate,middle_inflow,middle_inflow_rate,small_inflow,small_inflow_rate,trade_date)
                    persistence.insert('insert into stock_section_statistics values (REPLACE(UUID(),"-",""),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [item])
    
    def unified_unit(self, amount):
        if (amount.endswith('亿')):
            return float(amount[:-1]) * 10000
        else:
            return float(amount[:-1])
        
#板块股票映射爬虫
class StockSectionMappingTask(CrawlerTask):
    
    _init_url = "https://data.eastmoney.com/bkzj/{}.html"
    
    def execute(self):
        web = webdriver.Chrome(ChromeDriverManager( ).install())
        persistence = DaoMysqlImpl()
        section_list = persistence.select("select section_code, section_name from stock_section order by section_code")
        if (len(section_list) > 0):
            for section_code in section_list:
                url = self._init_url.format(section_code[0])
                web.get(url)
                while True:
                    stock_list = []
                    stock_list = web.find_elements(By.XPATH, '//*[@id="dataview"]/div[2]/div[2]/table/tbody/tr/td[2]/a')
                    for stock in stock_list:
                        print(stock.text + '-' + section_code[0])
                        item = (section_code[0], stock.text)
                        try:
                            persistence.insert('insert into section_stock_mapping values (%s,%s)', [item])
                        except Exception:
                            print('')
                    try: 
                        next_bt = web.find_element(By.LINK_TEXT, '下一页')
                        next_bt.click()
                        time.sleep(5)
                    except Exception:
                        break    
        
    
if __name__ == '__main__':
    task = SectionInfoTask()
    # task = StockSectionMappingTask()
    # print(task.unified_unit('1.2亿'))
    # print(task.unified_unit('4356.25万'))
    # print(task.unified_unit('0.25%'))
    task.execute()