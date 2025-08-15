select
dt,
et,
count(1) pv
from ham_jituan.dwd_client_event_di 
where dt between '2023-07-01' and '2023-07-18'
and wt_es like 'https://h.app.coc.10086.cn/cmcc-app/homeNew/homeNewIndexPagePlus.html%'
and et = 'clk'
group by dt,et order by dt ;


select
dt,
et,
count(1) pv
from ham_jituan.dwd_client_event_gio_di
where dt between '2023-07-01' and '2023-07-18'
and wt_es like 'https://h.app.coc.10086.cn/cmcc-app/homeNew/homeNewIndexPagePlus.html%'
and et = 'clk' and event_type='custom_event'
and trmnl_style in ('b95440ef47ec01fc','90be4403373b6463')
group by dt,et order by dt ;

event_all

select
toDate(event_time) as dt,
event_key as et,
count(1) pv
from olap.event_all
where toDate(event_time) between '2023-07-01' and '2023-07-13'
and if(position(domain,'.')>0,if(match(domain,'^[0-9]+[.]+[0-9]+[.]+[0-9]+[.]+[0-9]+$'),concat('http://',ifNull(domain,''),ifNull(attributes['$path'],'')),concat('https://',ifNull(domain,''),ifNull(attributes['$path'],''))),domain) like 'https://h.app.coc.10086.cn/cmcc-app/homeNew/homeNewIndexPagePlus.html%'
and et = 'clk' and event_type='custom_event'
and data_source_id in ('b95440ef47ec01fc','90be4403373b6463')
group by dt,et order by dt format CSVWithNames;


ck
if(position(domain,'.')>0,if(match(domain,'^[0-9]+[.]+[0-9]+[.]+[0-9]+[.]+[0-9]+$'),concat('http://',ifNull(domain,''),ifNull(attributes['$path'],'')),concat('https://',ifNull(domain,''),ifNull(attributes['$path'],''))),domain) as url,

