set hive.default.fileformat = TextFile;
set hive.execution.engine = mr;
set hive.vectorized.execution.enabled  = false;

insert overwrite table ham.dwd_monitor_url_dh_t partition (tp='jyh5',dt = '${DT}', hour = '${HH}')
select 
'jyh5' as type
,regexp_replace(dt, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as day
,regexp_replace(hour, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as hh
,regexp_replace(url,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','')
,count(1) as pv
,count(distinct ck_id) as uv
from ham.dwd_dcslog_event_di
where dt = '${DT}'
and hour = '${HH}'
group by dt,hour,url
;
