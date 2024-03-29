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
package VARCHAR(64),
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
param VARCHAR(8),
version VARCHAR(10),
type VARCHAR(10),
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
max  DECIMAL(15,5),
min  DECIMAL(15,5),
scope DECIMAL(15,5),
mean DECIMAL(15,5),
median DECIMAL(15,5),
std DECIMAL(15,5),
var DECIMAL(15,5),
ptile10 DECIMAL(15,5),
ptile20 DECIMAL(15,5),
ptile30 DECIMAL(15,5),
ptile40 DECIMAL(15,5),
ptile50 DECIMAL(15,5),
ptile60 DECIMAL(15,5),
ptile70 DECIMAL(15,5),
ptile80 DECIMAL(15,5),
ptile90 DECIMAL(15,5),
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
param_value VARCHAR(10),
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

CREATE TABLE IF NOT EXISTS factor_combination
(
id VARCHAR(128),
remark VARCHAR(64),
PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS factor_combination_mapping
(
id VARCHAR(32),
combination_id VARCHAR(128),
case_id VARCHAR(128),
param VARCHAR(10),
PRIMARY KEY(id)
);

insert into factor_case values ('MeanInflectionPoint_5__v1.0_20210101_20210929','factor.trend_factor','MeanInflectionPoint','v1.0','5','','20210101','20210929');
insert into factor_case values ('RisingTrend_v1.0_5|10_0.8|0.7__','factor.my_factor','RisingTrend','v1.0','5|10','0.8|0.7','','');
insert into factor_case values ('MACDPenetration_v1.0_12|16|9___','factor.momentum_factor','MACDPenetration','v1.0','12|16|9','','','');
insert into factor_case values ('RSIRegression_v1.0_14___','factor.momentum_factor','RSIRegression','v1.0','14','','','');
insert into factor_case values ('RSIGoldenCross_v1.0_7|14__','factor.my_factor','RSIGoldenCross','v1.0','7|14','','','');

insert into factor_combination values ('combination1','MACDPenetration|RSIRegression');
insert into factor_combination_mapping values ('aa','combination1','MACDPenetration_v1.0_12|16|9___','');
insert into factor_combination_mapping values ('bb','combination1','RSIRegression_v1.0_14___','14');

insert into factor_combination values ('combination2','MACDPenetration|RSIGoldenCross');
insert into factor_combination_mapping values ('cc','combination2','MACDPenetration_v1.0_12|16|9___','');
insert into factor_combination_mapping values ('dd','combination2','RSIGoldenCross_v1.0_7|14___','');

insert into learning_model values ('1', '{"training_set":[{"ts_code":"000858.SZ","start_date":"20190701","end_date":"20200310"}],"profit_period":"5","pre_process":[],"algorithm":"LinearRegression"}', '震荡行情');
insert into learning_model values ('2', '{"training_set":[{"ts_code":"000858.SZ","start_date":"20200325","end_date":"20210105"}],"profit_period":"5","pre_process":[],"algorithm":"LinearRegression"}', '攀升行情');
insert into learning_model values ('3', '{"training_set":[{"ts_code":"000858.SZ","start_date":"20200525","end_date":"20210901"}],"profit_period":"5","pre_process":[],"algorithm":"LinearRegression"}', '下降行情');

create view factor_ret_distribution_view as 
select 'ret1', t1.factor_case, t1.param_value, t2.max, t2.min, t2.scope, t2.mean, t2.median, t2.std, t2.var, t2.ptile10, t2.ptile20, t2.ptile30, t2.ptile40, t2.ptile50, t2.ptile60, t2.ptile70, t2.ptile80, t2.ptile90 from factor_ret_distribution t1, distribution_result t2 where t1.ret1 = t2.related_id
union all
select 'ret2', t1.factor_case, t1.param_value, t2.max, t2.min, t2.scope, t2.mean, t2.median, t2.std, t2.var, t2.ptile10, t2.ptile20, t2.ptile30, t2.ptile40, t2.ptile50, t2.ptile60, t2.ptile70, t2.ptile80, t2.ptile90 from factor_ret_distribution t1, distribution_result t2 where t1.ret2 = t2.related_id
union all
select 'ret3', t1.factor_case, t1.param_value, t2.max, t2.min, t2.scope, t2.mean, t2.median, t2.std, t2.var, t2.ptile10, t2.ptile20, t2.ptile30, t2.ptile40, t2.ptile50, t2.ptile60, t2.ptile70, t2.ptile80, t2.ptile90 from factor_ret_distribution t1, distribution_result t2 where t1.ret3 = t2.related_id
union all
select 'ret4', t1.factor_case, t1.param_value, t2.max, t2.min, t2.scope, t2.mean, t2.median, t2.std, t2.var, t2.ptile10, t2.ptile20, t2.ptile30, t2.ptile40, t2.ptile50, t2.ptile60, t2.ptile70, t2.ptile80, t2.ptile90 from factor_ret_distribution t1, distribution_result t2 where t1.ret4 = t2.related_id
union all
select 'ret5', t1.factor_case, t1.param_value, t2.max, t2.min, t2.scope, t2.mean, t2.median, t2.std, t2.var, t2.ptile10, t2.ptile20, t2.ptile30, t2.ptile40, t2.ptile50, t2.ptile60, t2.ptile70, t2.ptile80, t2.ptile90 from factor_ret_distribution t1, distribution_result t2 where t1.ret5 = t2.related_id
union all
select 'ret6', t1.factor_case, t1.param_value, t2.max, t2.min, t2.scope, t2.mean, t2.median, t2.std, t2.var, t2.ptile10, t2.ptile20, t2.ptile30, t2.ptile40, t2.ptile50, t2.ptile60, t2.ptile70, t2.ptile80, t2.ptile90 from factor_ret_distribution t1, distribution_result t2 where t1.ret6 = t2.related_id
union all
select 'ret7', t1.factor_case, t1.param_value, t2.max, t2.min, t2.scope, t2.mean, t2.median, t2.std, t2.var, t2.ptile10, t2.ptile20, t2.ptile30, t2.ptile40, t2.ptile50, t2.ptile60, t2.ptile70, t2.ptile80, t2.ptile90 from factor_ret_distribution t1, distribution_result t2 where t1.ret7 = t2.related_id
union all
select 'ret8', t1.factor_case, t1.param_value, t2.max, t2.min, t2.scope, t2.mean, t2.median, t2.std, t2.var, t2.ptile10, t2.ptile20, t2.ptile30, t2.ptile40, t2.ptile50, t2.ptile60, t2.ptile70, t2.ptile80, t2.ptile90 from factor_ret_distribution t1, distribution_result t2 where t1.ret8 = t2.related_id
union all
select 'ret9', t1.factor_case, t1.param_value, t2.max, t2.min, t2.scope, t2.mean, t2.median, t2.std, t2.var, t2.ptile10, t2.ptile20, t2.ptile30, t2.ptile40, t2.ptile50, t2.ptile60, t2.ptile70, t2.ptile80, t2.ptile90 from factor_ret_distribution t1, distribution_result t2 where t1.ret9 = t2.related_id
union all
select 'ret10', t1.factor_case, t1.param_value, t2.max, t2.min, t2.scope, t2.mean, t2.median, t2.std, t2.var, t2.ptile10, t2.ptile20, t2.ptile30, t2.ptile40, t2.ptile50, t2.ptile60, t2.ptile70, t2.ptile80, t2.ptile90 from factor_ret_distribution t1, distribution_result t2 where t1.ret10 = t2.related_id;