

自己写的
select 
attributes['WT_et'] as et,
decodeURLComponent(attributes['WT_event']) as event,
decodeURLComponent(attributes['WT_es']) as es,
toDateTime(event_time) as event_time,
case when match(user, 'g-9l/11$') then user end as mobile
FROM olap.event_all
where mobile = '13814022856'
and event_time='2023-05-24'
;


新年给的
gio


select 
case when match(user, '^[0-9]{11}$') then user end AS mobile,
anonymous_user AS ck_id,
session  AS ss_id,
ip,
attributes['$reffer'] as referer,
decodeURLComponent(attributes['WT_event']) AS event,
if(position(domain,'.')>0,if(match(domain,'^[0-9]+[.]+[0-9]+[.]+[0-9]+[.]+[0-9]+$'),concat('http://',ifNull(domain,''),ifNull(attributes['$path'],'')),concat('https://',ifNull(domain,''),ifNull(attributes['$path'],''))),domain) as url,
data_source_id as trmnl_style,
attributes['$title'] as ti,
event_key as et, 
attributes['$duration'] as ed,
decodeURLComponent(attributes['WT_XY']) as xy,
concat(url,ifNull(concat('?',attributes['$query']),'')) as es,
toDate(event_time) as dt,
if(length(cast(toHour(toDateTime(event_time)) as String))=1,concat('0',cast(toHour(toDateTime(event_time)) as String)),cast(toHour(toDateTime(event_time)) as String)) as hour,
data_source_id
FROM olap.event_all
WHERE dt = '2023-05-24'
--and data_source_id in ('a7464be8b200fe24','dcso02mhurci7zvoq6fa18exaf3_3s3i')
and mobile = '13814022856'
and es like '%https://xcxp.bmcc.com.cn/rights-front/dist/index.html#/activity/FlowCarnival%'
FORMAT CSVWithNames;

select 
*
from olap.event_all
WHERE toDate(event_time) = '2023-05-24'
and user = '13814022856'
FORMAT CSVWithNames;

select 
*
from olap.event_all
WHERE toDate(event_time) = '2023-05-24'
--and user = '13948869613'
and attributes['WT_co_f']='258633e87c0fe0d80181684898978928'
FORMAT CSVWithNames;

select 
*
from olap.event_all
WHERE toDate(event_time) = '2023-05-24'
--and user = '13948869613'
and toDateTime(client_time) ='1684899257'
FORMAT CSVWithNames;

toDatime('2023-05-24 11:03:09.000')



formatDateTime(toDateTime(attributes['dcsdat']),'%Y-%m-%d %H:%M:00')



select 
*
from olap.event_all
WHERE toDate(event_time) = '2023-05-24'
--and user = '13948869613'


FORMAT CSVWithNames;




博新
select 
--case when match(user, '^[0-9]{11}$') then user end AS mobile,
attributes['WT_mobile'] as mobile,
anonymous_user AS ck_id,
session  AS ss_id,
ip,
attributes['$reffer'] as referer,
decodeURLComponent(attributes['WT_event']) AS event,
if(position(domain,'.')>0,if(match(domain,'^[0-9]+[.]+[0-9]+[.]+[0-9]+[.]+[0-9]+$'),concat('http://',ifNull(domain,''),ifNull(attributes['$path'],'')),concat('https://',ifNull(domain,''),ifNull(attributes['$path'],''))),domain) as url,
data_source_id as trmnl_style,
attributes['$title'] as ti,
event_key as et, 
attributes['$duration'] as ed,
decodeURLComponent(attributes['WT_XY']) as xy,
--concat(url,ifNull(concat('?',attributes['$query']),'')) as es,
decodeURLComponent(attributes['WT_es']) as es,
toDate(event_time) as dt,
if(length(cast(toHour(toDateTime(event_time)) as String))=1,concat('0',cast(toHour(toDateTime(event_time)) as String)),cast(toHour(toDateTime(event_time)) as String)) as hour,
--attributes['WT_vtid'] as vtid,
data_source_id
FROM olap.event_all
WHERE dt = '2023-05-24'
--and data_source_id in ('a7464be8b200fe24','dcso02mhurci7zvoq6fa18exaf3_3s3i')
and mobile = '13814022856'
--and vtid = '2cbe1a5d3464af080351684892647218'
--and attributes['tid'] = '8663702051299860480200-586950634bi0600010000000000'
and es like '%https://xcxp.bmcc.com.cn/rights-front/dist/index.html#/activity/FlowCarnival%'
FORMAT CSVWithNames;