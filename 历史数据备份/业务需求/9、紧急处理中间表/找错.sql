select goods_no
from webtrends.event_hi_dcslog_all
where dt='2023-04-25' and hour = '09' and goods_no is not null;


select
attributes['WT_next_pageid'] AS next_pageid,
decodeURLComponent(attributes['WT_next_pagename']) AS next_pagename,
attributes['WT_touchcode'] AS touchcode,
attributes['WT_point_name'] AS point_name,
attributes['WT_component_name'] AS component_name,
attributes['WT_serial_no'] as serial_no,
attributes['WT_act_str_step_id'] as act_str_step_id,
attributes['WT_group_id'] as group_id,
attributes['WT_mr_id'] as mr_id,
attributes['WT_ad_step'] as ad_step,
attributes['WT_touch_tm'] as touch_tm,
attributes['WT_ed'] as ed,
decodeURLComponent(attributes['WT_vt_sid']) as vt_sid
FROM olap.event_all
WHERE toDate(event_time) = '2023-04-25' 
and next_pageid is not null 
and next_pagename is not null 
and touchcode is not null 
and point_name is not null 
and component_name is not null 
and serial_no is not null 
and act_str_step_id is not null 
and group_id is not null 
and mr_id is not null 
and ad_step is not null 
and touch_tm is not null 
and ed is not null 
and vt_sid is not null 
limit 10;


1、domain没有点 什么也不用拼
2、domain只有点和数字 拼 http://  + domain + path + ？ + query      168.25.6.3
3、domain 只有点和英文字母 拼 https://  + domain + path+ ？ + query
4、拼的逻辑：如果query没有值 不用拼 ？


select 
if(match(domain,'^\d*$'),concat('http://',ifNull(domain,''),ifNull(attributes['\$path'],'')),concat('https://',ifNull(domain,''),ifNull(attributes['\$path'],''))) as url,
ifNull(attributes['\$query'],'')

select 
domain,
attributes['$path'] as path,
attributes['$query'] as q,
if(position(domain,'.')>0,if(match(domain,'^[0-9]+[.]+[0-9]+[.]+[0-9]+[.]+[0-9]+$'),concat('http://',ifNull(domain,''),ifNull(attributes['$path'],'')),concat('https://',ifNull(domain,''),ifNull(attributes['$path'],''))),domain) as url,
concat(url,ifNull(concat('?',attributes['$query']),'')) as murl
FROM olap.event_all
WHERE toDate(event_time) = '2023-04-24' and q is not null limit 1;




























if(match('10.253.18.69','^[0-9]+[.]+[0-9]+[.]+[0-9]+[.]+[0-9]+$'),concat('http://',ifNull(domain,''),ifNull(attributes['\$path'],'')),concat('https://',ifNull(domain,''),ifNull(attributes['\$path'],'')))







ifNull(concat('?',attributes['$query']),'')





















concat(if(position(domain,'.')>0,if(match(domain,'^[0-9]+[.]+[0-9]+[.]+[0-9]+[.]+[0-9]+$'),concat('http://',ifNull(domain,''),ifNull(attributes['$path'],'')),concat('https://',ifNull(domain,''),ifNull(attributes['$path'],''))),domain),ifNull(concat('?',attributes['$query']),'')) as wt_es,