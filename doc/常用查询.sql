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