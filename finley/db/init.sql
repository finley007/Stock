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
param  VARCHAR(10),
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
status     VARCHAR(1),  --0?????????, 1?????????
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
--??????????????????
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
-- ???????????????
CREATE TABLE IF NOT EXISTS stock_statistics
(
trade_date VARCHAR(10),
rising_count INT,
falling_count  INT,
flat_count  INT,
PRIMARY KEY(trade_date)
);

-- real_time_tick
-- ?????????????????????
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
-- ?????????????????????
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


insert into factor_case values ('MeanInflectionPoint_5_20210101_20210929','MeanInflectionPoint','5','20210101','20210929');
insert into factor_case values ('MeanInflectionPoint_20_20210101_20211111','MeanInflectionPoint','20','20210101','20211111');
insert into factor_case values ('MeanPenetration_20_20210101_20210929','MeanPenetration','20','20210101','20210929');
insert into factor_case values ('MeanPenetration_20_20210101_20211126','MeanPenetration','20','20210101','20211126');
insert into factor_case values ('EnvelopePenetration_MeanPercentage_20_20210101_20211016','EnvelopePenetration_MeanPercentage','20','20210101','20211016');
insert into factor_case values ('MACDPenetration_0_20210101_20211023','MACDPenetration','0','20210101','20211023');
insert into factor_case values ('LowerHatch_5_20210101_20211102','LowerHatch','5','20210101','20211102');
insert into factor_case values ('MeanTrend_20_20210101_20211113','MeanTrend','20','20210101','20211113');
insert into factor_case values ('EnvelopePenetration_Keltner_20_20210101_20211115','EnvelopePenetration_Keltner','20','20210101','20211115');
insert into factor_case values ('AdvanceEnvelopePenetration_Keltner_20_20210101_20211120','AdvanceEnvelopePenetration_Keltner','20','20210101','20211120');
insert into factor_case values ('MeanInflectionPoint_10_20210101_20211214','MeanInflectionPoint','10','20210101','20211214');
insert into factor_case values ('KDJRegression_9_20210101_20211226','KDJRegression','9','20210101','20211226');
insert into factor_case values ('RSIPenetration_14_20210101_20211226','RSIPenetration','14','20210101','20211226');
insert into factor_case values ('DRFPenetration_0.3_20210101_20211231','DRFPenetration','0.3','20210101','20211231');
insert into factor_case values ('WRRegression_30_20210101_20220113','WRRegression','30','20210101','20220114');
insert into factor_case values ('UOPenetration_7|14|28_20210101_20220114','UOPenetration','7|14|28','20210101','20220114');
insert into factor_case values ('RVIPenetration_10_20210101_20220117','RVIPenetration','10','20210101','20220117');
insert into factor_case values ('FIPenetration_13_20210101_2022217','FIPenetration','13','20210101','20220217');
insert into factor_case values ('MFIPenetration_14_20210101_20220222','MFIPenetration','14','20210101','20220222');
insert into factor_case values ('MeanTrendFirstDiff_10_20210101_20220614','MeanTrendFirstDiff','10','20210101','20220614');

insert into learning_model values ('1', '{"training_set":[{"ts_code":"000858.SZ","start_date":"20190701","end_date":"20200310"}],"profit_period":"5","pre_process":[],"algorithm":"LinearRegression"}', '????????????');
insert into learning_model values ('2', '{"training_set":[{"ts_code":"000858.SZ","start_date":"20200325","end_date":"20210105"}],"profit_period":"5","pre_process":[],"algorithm":"LinearRegression"}', '????????????');
insert into learning_model values ('3', '{"training_set":[{"ts_code":"000858.SZ","start_date":"20200525","end_date":"20210901"}],"profit_period":"5","pre_process":[],"algorithm":"LinearRegression"}', '????????????');

select distinct factor_case from simulation_result;
select count(*) from simulation_result where factor_case = 'mean_trend_first_diff_10_20210101_20220614' and profit_rate > 0 and version = '1.0';
select count(*) from simulation_result where factor_case = 'mean_trend_first_diff_10_20210101_20220614' and profit_rate < 0 and version = '1.0';
select ts_code, profit_rate from simulation_result where factor_case = 'mean_trend_first_diff_10_20210101_20220614' and profit_rate > 0 and version = '1.0' order by profit_rate desc;
select ts_code, profit_rate from simulation_result where factor_case = 'mean_trend_first_diff_10_20210101_20220614' and profit_rate < 0 and version = '1.0' order by profit_rate desc;
select avg(profit_rate) from simulation_result where factor_case = 'mean_trend_first_diff_10_20210101_20220614' and version = '1.0';
delete from simulation_result where factor_case = 'mean_trend_first_diff_10_20210101_20220614';