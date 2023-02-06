select distinct factor_case from simulation_result;
select count(*) from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate > 0 and version = '1.0';
select count(*) from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate < 0 and version = '1.0';
select ts_code, profit_rate from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate > 0 and version = '1.0' order by profit_rate desc;
select ts_code, profit_rate from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate < 0 and version = '1.0' order by profit_rate desc;
select avg(profit_rate) from simulation_result where factor_case = 'fi_penetration_26_20210101_20220811' and version = '1.0';
delete from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810';

select * from factor_analysis order by factor_case;
select * from distribution_result where type = 1 and related_id = 'c930561014d943919ed0cba6c1383d59';
select * from factor_ret_distribution_view where factor_case = 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__' and param_value = '10';

select t3.* from factor_combination t1, factor_combination_mapping t2, factor_case t3 where t1.id = t2.combination_id and t2.case_id = t3.id and t1.id = 'factor_combination1';