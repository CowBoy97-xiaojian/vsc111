select   row_number() over(),
         regexp_replace(dt,'-',''),
         channel_id,
         count(distinct ck_id) uv 
from ham.dwd_dcslog_event_di where dt='${DT}' group by channel_id,dt;