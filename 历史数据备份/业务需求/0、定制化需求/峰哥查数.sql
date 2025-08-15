select
t1.trmnl_style,t1.dt,t1.hour,count(1)
from (
select
dt,hour,trmnl_style
from ham.dwd_dcslog_event_di
where dt = '2023-03-15'
union all
select
dt,hour,trmnl_style 
from ham_jituan.dwd_client_event_di
where dt = '2023-03-15'
)t1
where t1.hour = '01'
group by t1.dt,t1.trmnl_style,t1.hour
order by t1.dt,t1.hour;



select
t1.trmnl_style,t1.dt,count(1)
from (
select
dt,hour,trmnl_style
from ham.dwd_dcslog_event_di
where dt = '2023-03-15' and hour between '01' and '02'
union all
select
dt,hour,trmnl_style 
from ham_jituan.dwd_client_event_di
where dt = '2023-03-15' and hour between '01' and '02'
)t1
group by t1.dt,t1.trmnl_style
order by t1.dt;

ck_id
wt_co_f

--hive
select
t1.dt,t1.hour,count(1) as pv,count(distinct user) as uv
from (
select
dt,hour,trmnl_style,ck_id as user
from ham.dwd_dcslog_event_di
where dt = '2023-04-01'
union all
select
dt,hour,trmnl_style ,wt_co_f as user
from ham_jituan.dwd_client_event_di
where dt = '2023-04-01'
)t1
group by t1.dt,t1.hour
order by t1.dt,t1.hour;

--ck

select
toHour(event_time) hour,
count(1)
from olap.event_all
where toDate(event_time) = '2023-04-01'
group by hour
order by hour;

--中间表

select
t1.dt,t1.hour,count(1) as pv
from (
select
dt,hour,trmnl_style
from webtrends.event_hi_dcslog_all
where dt = '2023-04-01'
union all
select
dt,hour,trmnl_style 
from webtrends.event_hi_client_all
where dt = '2023-04-01'
)t1
group by t1.dt,t1.hour
order by t1.dt,t1.hour;