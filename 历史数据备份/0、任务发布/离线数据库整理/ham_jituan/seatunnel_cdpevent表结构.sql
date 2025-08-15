desc seatunnel_cdpevent;

col_name,data_type,comment
project_id,string,
event_key,string,
event_time,string,
event_type,string,
client_time,string,
anonymous_user,string,
user,string,
user_key,string,
session,string,
package,string,
platform,string,
referrer_domain,string,
browser,string,
browser_version,string,
os,string,
os_version,string,
client_version,string,
channel,string,
device_brand,string,
device_model,string,
device_type,string,
device_orientation,string,
language,string,
account_id,string,
domain,string,
ip,string,
user_agent,string,
sdk_version,string,
location_latitude,string,
location_longitude,string,
data_source_id,string,
esid,string,
attributes,string,
dt,string,
hour,string,
,NULL,NULL
# Partition Information,NULL,NULL
# col_name            ,data_type           ,comment             
,NULL,NULL
dt,string,
hour,string,

2024-02-22 00:00:00.027,2024-02-22 00:00:00.135
2024-02-22 00:00:00.082,2024-02-21 23:59:59.822

11-14数据补数校验

select 
dt,count(1)
from ham_jituan.seatunnel_cdpevent 
group by dt
;

select count(*)
from ham_jituan.seatunnel_cdpevent
where dt = '2024-02-11';
and event_time < client_time;