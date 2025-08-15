--ck-evenet_all
select count(1)
from olap.event_all
where toDate(event_time)='2023-08-25'
and toHour(event_time)='12'
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
from ods_client_event_gio_all
where dt='2023-08-25'
and hour='12'
and get_json_object(attributes,'$.WT_av') is null
and data_source_id in ('a1f48d9ff4f42571','b508a809cbbddd0b') 
and event_type='custom_event'
and (
    instr(get_json_object(attributes,'$.WT_mc_ev'),'QYCS')=0
    or get_json_object(attributes,'$.WT_mc_ev') is null
    or get_json_object(attributes,'$.WT_mc_ev') ='210315_QYCS'
    or get_json_object(attributes,'$.$path') like '%/coc3/canvas/rightsmarket-h5-canvas%'
)

--hive_dwd
select count(1)
from dwd_dcslog_event_gio_di
where dt='2023-08-25'
and hour='12'
and (trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b') 
and (
instr(mc_ev,'QYCS')=0 
or mc_ev is null
or mc_ev = '210315_QYCS'
or es like '%/coc3/canvas/rightsmarket-h5-canvas%'
))