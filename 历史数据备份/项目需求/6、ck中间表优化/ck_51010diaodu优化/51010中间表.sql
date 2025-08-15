
1、回填逻辑去掉
2、case when match(user, '^[0-9]{11}$') then user when match(user, '.*==$') then aes_function(user) end AS mobile

性能查询
select formatReadableSize(memory_usage) as neicun,event_time,query_duration_ms,query_kind,exception,type,query from system.query_log where toDate(event_time) = '2023-08-07' and toHour(event_time) = '1' order by neicun desc limit 1;


select  formatReadableSize(memory_usage) as neicun,event_time,query_duration_ms,query_kind,exception,type,query from system.query_log where toDate(event_time) = '2023-08-07' and toHour(event_time) = '1' and toMinute(event_time)='34' order by neicun desc limit 5;



select formatReadableSize(memory_usage) as neicun,event_time,query_duration_ms,query_kind,exception,type,query from system.query_log where toDate(event_time) = '2023-08-07' and toHour(event_time) = '1' and query like '%select * from olap.event_local%' order by neicun desc limit 1;



select formatReadableSize(memory_usage) as neicun  from system.query_log where toDate(event_time) = '2023-08-07' and toHour(event_time) = '1' and toMinute(event_time)='34'  and query like '%select * from olap.event_local%' order by neicun desc limit 5;

select formatReadableSize(memory_usage) as neicun ,query  from system.query_log where toDate(event_time) = '2023-08-07' and toHour(event_time) = '2'  and query like '%insert into table webtrends.event_51010_tmp_all%' order by neicun desc limit 5;

select formatReadableSize(sum(memory_usage)) from system.query_log where toDate(event_time) = '2023-08-07' and toHour(event_time) = '1' and query like '%select * from olap.event_local%';

formatReadableSize(memory_usage)



select formatReadableSize(sum(memory_usage)) from system.query_log where toDate(event_time) = '2023-08-07' and toHour(event_time) = '2' and toMinute(event_time)='34' and query like '%insert into table webtrends.event_51010_tmp_all%';

select formatReadableSize(sum(memory_usage)) from system.query_log where toDate(event_time) = '2023-08-07' and toHour(event_time) = '2' and query like '%insert into table webtrends.event_51010_tmp_all%' ;

select formatReadableSize(sum(memory_usage)) from system.query_log where toDate(event_time) = '2023-08-07' and toHour(event_time) = '1' and toMinute(event_time)='34' and query like '%select project_id,event_key,event_time%';

select formatReadableSize(memory_usage) as neicun,toDate(event_time),toHour(event_time),toMinute(event_time)  from system.query_log where toDate(event_time) = '2023-08-07' and toHour(event_time) = '1' and query like '%select project_id,event_key,event_time%' order by neicun desc limit 5;


insert into table webtrends.event_51010_tmp_all



select formatReadableSize(memory_usage) as neicun,toDate(event_time),toHour(event_time),toMinute(event_time)  from system.query_log where toDate(event_time) = '2023-08-19' and toHour(event_time) = '1' and query like '%coc3/canvas/rightsmarket-h5-canvas%' order by neicun desc limit 5;

select formatReadableSize(memory_usage) as neicun,toDate(event_time),toHour(event_time),toMinute(event_time)  from system.query_log where toDate(event_time) = '2023-08-19' and toHour(event_time) = '1' order by neicun desc limit 30;

select formatReadableSize(memory_usage) as neicun,toDate(event_time),toHour(event_time),toMinute(event_time) from system.query_log where toDate(event_time) = '2023-08-19' and toHour(event_time) = '1' and query like '%insert into table webtrends.event_51010_tmp_all%' order by neicun desc limit 30;

select formatReadableSize(memory_usage) as neicun,toDate(event_time),toHour(event_time),toMinute(event_time) from system.query_log where toDate(event_time) = '2023-08-19' and toHour(event_time) = '1' and type like '%Excepotion%' order by neicun desc limit 30;

select memory_usage,formatReadableSize(memory_usage) as neicun,toDate(event_time),toHour(event_time),toMinute(event_time),type from system.query_log where toDate(event_time) = '2023-08-19' and toHour(event_time) = '1' and type='ExceptionWhileProcessing' order by event_time limit 50 ;


select formatReadableSize(sum(memory_usage)) from system.query_log where toDate(event_time) = '2023-08-19' and toHour(event_time) = '1' and toMinute(event_time)='20';

select query_id,memory_usage,formatReadableSize(memory_usage) as neicun,toDate(event_time),toHour(event_time),toMinute(event_time),type,exception from system.query_log where toDate(event_time) = '2023-08-19' and toHour(event_time) = '1' and type='ExceptionWhileProcessing' order by event_time limit 50 ;


select query_id,query from system.query_log where toDate(event_time) = '2023-08-19' and toHour(event_time) = '1' and query_id='b86e5e3b-458f-4123-ad73-12dc58e89946';

b86e5e3b-458f-4123-ad73-12dc58e89946

query_id



