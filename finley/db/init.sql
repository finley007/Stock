-- stock
CREATE DATABASE IF NOT EXISTS stock;
use stock;

-- static_stock_list
CREATE TABLE IF NOT EXISTS static_stock_list
(
ts_code VARCHAR(10),
symbol VARCHAR(6),
name VARCHAR(20),
area  VARCHAR(10),
industry  VARCHAR(20),
fullname  VARCHAR(128),
enname  VARCHAR(128),
cnspell  VARCHAR(10),
market  VARCHAR(10),
exchange  VARCHAR(8),
curr_type  VARCHAR(3),
list_status  VARCHAR(1),
list_date  VARCHAR(8),
delist_date  VARCHAR(8),
is_hs  VARCHAR(6),
PRIMARY KEY(ts_code)
);

-- static_calendar
CREATE TABLE IF NOT EXISTS static_calendar
(
        id INT(11),
exchange VARCHAR(8),
cal_date VARCHAR(8),
is_open  VARCHAR(1),
PRIMARY KEY(id)
); 

-- analysis_result
CREATE TABLE IF NOT EXISTS analysis_result
(
id VARCHAR(32),
analysis_id VARCHAR(32),
factor_code VARCHAR(128),
factor_value  VARCHAR(20),
param_value  VARCHAR(10), 
ts_code  VARCHAR(10),
ts_link     VARCHAR(128),
create_date  VARCHAR(8),
1st_day_ret  VARCHAR(10),
2nd_day_ret  VARCHAR(10),
3rd_day_ret  VARCHAR(10),
4th_day_ret  VARCHAR(10),
5th_day_ret  VARCHAR(10),
unique(factor_code, param_value, ts_code, create_date)
);

-- learning_model
CREATE TABLE IF NOT EXISTS learning_model
(
id INT,
model   TEXT,
remark  VARCHAR(512),
PRIMARY KEY(id)
);

-- factor_case
CREATE TABLE IF NOT EXISTS factor_case
(
id VARCHAR(128),
factor VARCHAR(64),
version  VARCHAR(10),
param  VARCHAR(50),
threshold VARCHAR(50),
start_date  VARCHAR(8),
end_date  VARCHAR(8),
PRIMARY KEY(id)
);

-- simulation_result
CREATE TABLE IF NOT EXISTS simulation_result
(
id VARCHAR(32),
factor_case VARCHAR(128),
ts_code  VARCHAR(10),
start_date  VARCHAR(8),
end_date  VARCHAR(8),
trans_count INT,
profit_count  INT,
loss_count  INT,
max_profit  DECIMAL(10,2),
max_loss  DECIMAL(10,2),
max_profit_date VARCHAR(30),
max_loss_date VARCHAR(30),
profit DECIMAL(10,2),
profit_rate DECIMAL(10,2),
version VARCHAR(10),
created_date DATETIME not null default NOW(),
PRIMARY KEY(id)
);

-- correlation_result
CREATE TABLE IF NOT EXISTS correlation_result
(
id VARCHAR(32),
factor_code VARCHAR(128),
param_value  VARCHAR(10),
ts_code  VARCHAR(10),
ts_link     VARCHAR(128),
create_date  VARCHAR(8),
1st_day_ret  VARCHAR(10),
2nd_day_ret  VARCHAR(10),
3rd_day_ret  VARCHAR(10),
4th_day_ret  VARCHAR(10),
5th_day_ret  VARCHAR(10),
6th_day_ret  VARCHAR(10),
7th_day_ret  VARCHAR(10),
8th_day_ret  VARCHAR(10),
9th_day_ret  VARCHAR(10),
10th_day_ret  VARCHAR(10),
unique(factor_code, param_value, ts_code, create_date)
);

-- factor_correlation_result
CREATE TABLE IF NOT EXISTS factor_correlation_result
(
id VARCHAR(32),
factor_code1  VARCHAR(128),
param_value1  VARCHAR(10),
factor_code2  VARCHAR(128),
param_value2  VARCHAR(10),
ts_code       VARCHAR(10),
correlation   VARCHAR(10),
create_date   VARCHAR(8),
unique(factor_code1, param_value1, factor_code2, param_value2, ts_code, create_date)
);

--transation_record
CREATE TABLE IF NOT EXISTS transaction_record
(
id VARCHAR(32),
ts_code       VARCHAR(10),
volume        INT,
type       VARCHAR(10),  --LONG, SHORT
status     VARCHAR(1),  --0未平层, 1已平仓
open_price    DECIMAL(10,2),
open_date     VARCHAR(32),
close_price   DECIMAL(10,2),
close_date     VARCHAR(32),
stop_price   DECIMAL(10,2),
factor_code VARCHAR(128),
factor_value  VARCHAR(20),
param_value  VARCHAR(10),
profit DECIMAL(10,2),
profit_rate DECIMAL(10,2),
PRIMARY KEY(id)
);

-- future_instrument_list
--期货合约列表
CREATE TABLE IF NOT EXISTS future_instrument_list
(
product VARCHAR(10),
instrument  VARCHAR(6),
start_time VARCHAR(20),
end_time  VARCHAR(20),
trans_time_range VARCHAR(128),
unique(product, instrument)
);

-- stock_statistics
-- 股票统计表
CREATE TABLE IF NOT EXISTS stock_statistics
(
trade_date VARCHAR(10),
rising_count INT,
falling_count  INT,
flat_count  INT,
PRIMARY KEY(trade_date)
);

-- real_time_tick
-- 实时交易数据表
CREATE TABLE IF NOT EXISTS real_time_tick
(
  tick timestamp(6),
  instrument_id VARCHAR(20),
  last_price FLOAT,
  open_interest INT,
  open_interest_delta INT,
  trade_turnover bigint,
  trade_volume INT,
  entry_volume INT,
  exit_volume INT,
  trade_type smallint,
  bid_price1 FLOAT,
  bid_price2 FLOAT,
  bid_price3 FLOAT,
  bid_price4 FLOAT,
  bid_price5 FLOAT,
  ask_price1 FLOAT,
  ask_price2 FLOAT,
  ask_price3 FLOAT,
  ask_price4 FLOAT,
  ask_price5 FLOAT,
  bid_volume1 INT,
  bid_volume2 INT,
  bid_volume3 INT,
  bid_volume4 INT,
  bid_volume5 INT,
  ask_volume1 INT,
  ask_volume2 INT,
  ask_volume3 INT,
  ask_volume4 INT,
  ask_volume5 INT,
  pre_delta FLOAT,
  curr_delta FLOAT,
  upper_limit_price FLOAT,
  lower_limit_price FLOAT,
  PRIMARY KEY (tick, instrument_id)
);

-- real_time_tick
-- 实时交易数据表
CREATE TABLE IF NOT EXISTS 1_min_k_line
(
time    timestamp(6),
open        FLOAT,
close            FLOAT,
low              FLOAT,
high             FLOAT,
volume           FLOAT,
open_interest    FLOAT,
product           VARCHAR(10),
instrument        VARCHAR(20),
PRIMARY KEY (time, instrument)
);

-- stock_section
-- 股票板块表
CREATE TABLE IF NOT EXISTS stock_section
(
enterprise VARCHAR(4),  
section_code VARCHAR(10),
section_name VARCHAR(20),
remark  VARCHAR(512),
PRIMARY KEY(enterprise, section_code)
);

-- stock_section_statistics
-- 股票板块信息表
CREATE TABLE IF NOT EXISTS stock_section_statistics
(
id VARCHAR(32),
section_code VARCHAR(10),
section_name VARCHAR(20),
ranking INT,
main_inflow DECIMAL(10,2), 
main_inflow_rate DECIMAL(10,2), 
super_large_inflow DECIMAL(10,2),
super_large_inflow_rate DECIMAL(10,2),
large_inflow DECIMAL(10,2),
large_inflow_rate DECIMAL(10,2),
middle_inflow DECIMAL(10,2),
middle_inflow_rate DECIMAL(10,2),
small_inflow DECIMAL(10,2),
small_inflow_rate DECIMAL(10,2),
trade_date VARCHAR(10),
PRIMARY KEY(id)
);

-- section_stock_mapping
-- 股票板块股票映射表
CREATE TABLE IF NOT EXISTS section_stock_mapping
(
section_code VARCHAR(10),
ts_code       VARCHAR(10),
PRIMARY KEY(ts_code, section_code)
);

-- test 测试表
CREATE TABLE IF NOT EXISTS test
(
varchar_column VARCHAR(10),
int_column  int(10),
date_column  date,
datetime_column  datetime,
time_column  time,
created_time datetime,
modified_time datetime,
PRIMARY KEY(varchar_column)
);

-- 分布结果表
CREATE TABLE IF NOT EXISTS distribution_result
(
id VARCHAR(32),
type int comment '0-因子分布，1-因子收益分布',
related_id  VARCHAR(32),
max  DECIMAL(10,5),
min  DECIMAL(10,5),
scope DECIMAL(10,5),
mean DECIMAL(10,5),
median DECIMAL(10,5),
std DECIMAL(10,5),
var DECIMAL(10,5),
file_path VARCHAR(128),
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);

-- 因子分析表
CREATE TABLE IF NOT EXISTS factor_analysis
(
id VARCHAR(32),
factor_case VARCHAR(128),
filters  VARCHAR(128),
param_value VARCHAR(10),
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);

-- 因子收益分布表
CREATE TABLE IF NOT EXISTS factor_ret_distribution
(
id VARCHAR(32),
factor_case VARCHAR(128),
filters  VARCHAR(128),
ret1  VARCHAR(32),
ret2  VARCHAR(32),
ret3  VARCHAR(32),
ret4  VARCHAR(32),
ret5  VARCHAR(32),
ret6  VARCHAR(32),
ret7  VARCHAR(32),
ret8  VARCHAR(32),
ret9  VARCHAR(32),
ret10  VARCHAR(32),
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);

insert into factor_case values ('MeanInflectionPoint_5__v1.0_20210101_20210929','MeanInflectionPoint','v1.0','5','','20210101','20210929');
insert into factor_case values ('RisingTrend_v1.0_5|10_0.8|0.7__','RisingTrend','v1.0','5|10','0.8|0.7','','');

insert into learning_model values ('1', '{"training_set":[{"ts_code":"000858.SZ","start_date":"20190701","end_date":"20200310"}],"profit_period":"5","pre_process":[],"algorithm":"LinearRegression"}', '震荡行情');
insert into learning_model values ('2', '{"training_set":[{"ts_code":"000858.SZ","start_date":"20200325","end_date":"20210105"}],"profit_period":"5","pre_process":[],"algorithm":"LinearRegression"}', '攀升行情');
insert into learning_model values ('3', '{"training_set":[{"ts_code":"000858.SZ","start_date":"20200525","end_date":"20210901"}],"profit_period":"5","pre_process":[],"algorithm":"LinearRegression"}', '下降行情');

select distinct factor_case from simulation_result;
select count(*) from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate > 0 and version = '1.0';
select count(*) from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate < 0 and version = '1.0';
select ts_code, profit_rate from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate > 0 and version = '1.0' order by profit_rate desc;
select ts_code, profit_rate from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate < 0 and version = '1.0' order by profit_rate desc;
select avg(profit_rate) from simulation_result where factor_case = 'fi_penetration_26_20210101_20220811' and version = '1.0';
delete from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810';