--ck-evenet_all
select count(1)
from olap.event_all
where toDate(event_time)='2023-08-30'
and toHour(event_time)='16'
and event_type='custom_event'
and (data_source_id in ('a1f48d9ff4f42571','b508a809cbbddd0b') 
and attributes['WT_av'] is null
and (
    match(attributes['WT_mc_ev'],'QYCS')=0 
    or attributes['WT_mc_ev'] is null 
    or attributes['WT_mc_ev']='210315_QYCS') 
    or attributes['$path'] like '%/coc3/canvas/rightsmarket-h5-canvas%'
)



--hive_ods
select count(1)
from ham_jituan.ods_client_event_gio_all
where dt='2023-08-30'
and hour='16'
and get_json_object(attributes,'$.WT_av') is null
and data_source_id in ('a1f48d9ff4f42571','b508a809cbbddd0b') 
and event_type='custom_event'
and (
    instr(get_json_object(attributes,'$.WT_mc_ev'),'QYCS')=0
    or get_json_object(attributes,'$.WT_mc_ev') is null
    or get_json_object(attributes,'$.WT_mc_ev') ='210315_QYCS'
    or get_json_object(attributes,'$.$path') like '%/coc3/canvas/rightsmarket-h5-canvas%'
);
4321515

--hive_dwd
select count(1)
from dwd_dcslog_event_gio_di
where dt='2023-08-30'
and hour='16'
and (trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b') 
and (
instr(mc_ev,'QYCS')=0 
or mc_ev is null
or mc_ev = '210315_QYCS'
or es like '%/coc3/canvas/rightsmarket-h5-canvas%'
));
4321515


--domain整合sql
select count(1)
from (select
tb1.user,
tb1.user_key,
tb1.event_time,
tb1.domain as tb1d,
tb2.domain as tb2d,
tb1.data_source_id as dd1,
tb2.data_source_id as dd2, 
tb2.ture_datasourceid,
tb1.attributes as attributes
from 
(select 
user,
user_key,
event_time,
domain,
attributes,
data_source_id
from ham_jituan.ods_client_event_gio_all 
where dt = '2023-08-30'
and hour = '16'
and data_source_id in ('a1f48d9ff4f42571','b508a809cbbddd0b')
and get_json_object(attributes,'$.WT_av') is null 
and event_type = 'custom_event') tb1
inner join (
select
domain,
data_source_id,
ture_datasourceid
from ham.dim_domain_datasourceid_di
where ture_datasourceid in ('aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2')) tb2 
on tb1.domain=tb2.domain
where 
--instr(get_json_object(attributes,'$.WT_mc_ev'),'QYCS')=0
--or get_json_object(attributes,'$.WT_mc_ev') is null
get_json_object(attributes,'$.WT_mc_ev') ='210315_QYCS'
or get_json_object(attributes,'$.$path') like '%/coc3/canvas/rightsmarket-h5-canvas%';
) tb3;






--ods:4371851
select 
count(1)
from ham_jituan.ods_client_event_gio_all 
where dt = '2023-08-30'
and hour = '16'
and data_source_id in ('a1f48d9ff4f42571','b508a809cbbddd0b')
and get_json_object(attributes,'$.WT_av') is null 
and event_type = 'custom_event';




--一期domain去重
select
count(1)
from 
(select 
user,
user_key,
event_time,
domain,
attributes
from ham_jituan.ods_client_event_gio_all 
where dt = '2023-08-30'
and hour = '16'
and data_source_id in ('a1f48d9ff4f42571','b508a809cbbddd0b')
and get_json_object(attributes,'$.WT_av') is null 
and event_type = 'custom_event') tb1
inner join (
select
distinct domain
from ham.dim_domain_datasourceid_di
where ture_datasourceid in ('aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2')) tb2 
on tb1.domain=tb2.domain
where 
--instr(get_json_object(attributes,'$.WT_mc_ev'),'QYCS')=0
--or get_json_object(attributes,'$.WT_mc_ev') is null
get_json_object(attributes,'$.WT_mc_ev') ='210315_QYCS'
or get_json_object(attributes,'$.$path') like '%/coc3/canvas/rightsmarket-h5-canvas%';
;







--二期domian:
--4-1698143
--2-1676292
select count(1)
from (
    
    
    
select
tb1.user,
tb1.user_key,
tb1.event_time,
tb1.domain as tb1d,
tb2.domain as tb2d,
tb1.data_source_id as dd1,
tb2.interface,
tb1.attributes as attributes
from 
(select 
user,
user_key,
event_time,
domain,
attributes,
data_source_id
from ham_jituan.ods_client_event_gio_all 
where dt = '2023-09-10'
and hour = '16'
and data_source_id in ('a1f48d9ff4f42571','b508a809cbbddd0b')
and get_json_object(attributes,'$.WT_av') is null 
and event_type = 'custom_event') tb1
inner join (
select
domain,
interface
from ham.dim_domain_interface_di
where interface = '51006') tb2 
on tb1.domain=tb2.domain












where 
--instr(get_json_object(attributes,'$.WT_mc_ev'),'QYCS')=0
--or get_json_object(attributes,'$.WT_mc_ev') is null
get_json_object(attributes,'$.WT_mc_ev') ='210315_QYCS'
or get_json_object(attributes,'$.$path') like '%/coc3/canvas/rightsmarket-h5-canvas%'
) tb3;



--精准的查询ods
--4-7391574
--2-1676292
select count(1)
from ham_jituan.ods_client_event_gio_all
where dt='2023-09-10'
and hour='16'
and get_json_object(attributes,'$.WT_av') is null
and data_source_id in ('a1f48d9ff4f42571','b508a809cbbddd0b') 
and event_type='custom_event'
and (

or get_json_object(attributes,'$.WT_mc_ev') ='210315_QYCS'
or get_json_object(attributes,'$.$path') like '%/coc3/canvas/rightsmarket-h5-canvas%'
);


第一步 确定包含权益的数据