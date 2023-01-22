select distinct factor_case from simulation_result;
select count(*) from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate > 0 and version = '1.0';
select count(*) from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate < 0 and version = '1.0';
select ts_code, profit_rate from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate > 0 and version = '1.0' order by profit_rate desc;
select ts_code, profit_rate from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810' and profit_rate < 0 and version = '1.0' order by profit_rate desc;
select avg(profit_rate) from simulation_result where factor_case = 'fi_penetration_26_20210101_20220811' and version = '1.0';
delete from simulation_result where factor_case = 'fi_penetration_26_20210101_20220810';

select * from factor_analysis;
select * from distribution_result where type = 1;
select * from factor_ret_distribution;


select 'ret1', t2.* from factor_ret_distribution t1, distribution_result t2 where t1.ret1 = t2.related_id and t1.factor_case = 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__' and t1.param_value = '10'
union all
select 'ret2', t2.* from factor_ret_distribution t1, distribution_result t2 where t1.ret2 = t2.related_id and t1.factor_case = 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__' and t1.param_value = '10'
union all
select 'ret3', t2.* from factor_ret_distribution t1, distribution_result t2 where t1.ret3 = t2.related_id and t1.factor_case = 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__' and t1.param_value = '10'
union all
select 'ret4', t2.* from factor_ret_distribution t1, distribution_result t2 where t1.ret4 = t2.related_id and t1.factor_case = 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__' and t1.param_value = '10'
union all
select 'ret5', t2.* from factor_ret_distribution t1, distribution_result t2 where t1.ret5 = t2.related_id and t1.factor_case = 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__' and t1.param_value = '10'
union all
select 'ret6', t2.* from factor_ret_distribution t1, distribution_result t2 where t1.ret6 = t2.related_id and t1.factor_case = 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__' and t1.param_value = '10'
union all
select 'ret7', t2.* from factor_ret_distribution t1, distribution_result t2 where t1.ret7 = t2.related_id and t1.factor_case = 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__' and t1.param_value = '10'
union all
select 'ret8', t2.* from factor_ret_distribution t1, distribution_result t2 where t1.ret8 = t2.related_id and t1.factor_case = 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__' and t1.param_value = '10'
union all
select 'ret9', t2.* from factor_ret_distribution t1, distribution_result t2 where t1.ret9 = t2.related_id and t1.factor_case = 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__' and t1.param_value = '10'
union all
select 'ret10', t2.* from factor_ret_distribution t1, distribution_result t2 where t1.ret10 = t2.related_id and t1.factor_case = 'FallingTrend_v1.0_10|15|20_0.9|0.8|0.7__' and t1.param_value = '10';