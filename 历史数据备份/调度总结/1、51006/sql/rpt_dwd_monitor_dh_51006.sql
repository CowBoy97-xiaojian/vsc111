set hive.default.fileformat = Orc;
set hive.execution.engine = mr;

insert into table ham.dwd_monitor_dh partition(type = '51006')
select dt
,hour
,count(1)
,'hour' as cycle
,unix_timestamp() as timestamp
from ham.ads_rpt_hachi_51006_dt_hour
where dt = '${DT}'
and hour = '${HH}'
group by dt,hour;

alter table ham.dwd_monitor_dh partition (type='client') concatenate;
alter table ham.dwd_monitor_dh partition (type='51006') concatenate;
