-- 2022-04-11 09:20:00-2022-04-11 09:30:00
-- 4905.0-4903.0
delete from real_time_tick;
insert into real_time_tick select * from real_time_tick_bak;
delete from 1_min_k_line;
-- create 1_min_k_line
-- | max(tick)                  | min(tick)                  |
-- | 2022-04-12 09:03:28.500000 | 2022-04-10 23:00:00.500000 |
delete from real_time_tick where tick >= '2022-04-11 09:18:00';
delete from 1_min_k_line where time >= '2022-04-11 09:18:00';