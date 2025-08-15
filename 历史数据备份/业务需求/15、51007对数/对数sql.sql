select 
et,
count(1)
from webtrends.event_hi_client_all
where et='imp' and imp_type ='once'
and dt = '2023-05-28'
group by et;


select 
count(1)
from webtrends.event_hi_client_all
where et='imp'
and dt = '2023-05-28'
group by et;

select 
count(1)
from webtrends.event_hi_client_all
where et <> 'imp'
and dt = '2023-05-28';

select 
count(1)
from webtrends.event_hi_client_all
where et='clk'
and dt = '2023-05-28'
group by et;


clickhouse

imp
65940260

总数
1295061711

clk
62703103

<>imp
1229121451


hive

302140373

clk
48740347

select 
count(1)
from ham_jituan.dwd_client_event_di
where et <> 'imp'
and dt = '2023-05-28';

select 
count(1)
from ham_jituan.dwd_client_event_di
where et = 'clk'
and dt = '2023-05-28';

select 
count(1)
from ham_jituan.dwd_client_event_di
where et = 'imp'
and dt = '2023-05-28';

select hour,count(1) 
from ham_jituan.dwd_client_event_di
where dt = '2023-05-27'
group by dt,hour;




查数第二版


│ area_id            │ Nullable(String) │              │                    │         │                  │                │
│ imp_type

select 
hour,
count(1)
from webtrends.event_hi_client_all
where dt = '2023-06-02'
and trmnl_style in ('H5','ANDROID','iOS','MPX_App')
group by dt,hour
FORMAT CSVWithNames;

select
count(1)
from (select dcsdat,user_agent 
from ham_jituan.dwd_client_event_di
where dt = '2023-05-28'
group by dcsdat,user_agent);

select
count(1)
from 
(select dcsdat 
from ham_jituan.dwd_client_event_di where dt = '2023-05-28' group by dcsdat,user_agent,event_key);
dcsdat, user_agent, event_key, event_id, gio_id
客户端点击时间戳


dcs47gbrugonmg3u1x8njabg1_2p4f
dcs0cxkozfonmgrs8gfnw57g1_2e4p
dcsgswzxehonmgrc8hz5w67g1_9o7q
dcs4311adgonmgj23shb4oqyy_5q5k

select hour,count(1) from ham_jituan.dwd_client_event_di where dt = '2023-06-17' group by hour;

select count(1),count(1)/800000 from ham_jituan.dwd_client_event_di where dt = '2023-06-18';