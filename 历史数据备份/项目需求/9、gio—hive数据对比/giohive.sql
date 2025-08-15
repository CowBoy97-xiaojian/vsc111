
select 
carrieroperator
from ham_jituan.dwd_client_event_di
where
dt = '2023-07-11'
and hour = '11'
and
(
carrieroperator is not null
)
limit 3;





select 
dcsdat,dsad
from ham_jituan.dwd_client_event_gio_di  
where dt = '2023-07-11' 
and hour = '11'
and (
dcsdat is not null
or dcsdat is not null
)
limit 5;

ct,sr,vt_sid,userbrand



dcsdat

select 
attributes
from ham_jituan.dwd_client_event_gio_di  
where dt = '2023-07-11' 
and hour = '11'
and instr(attributes, 'extendid') > 0
limit 5;

wt_areaname

attributes['WT_ets']

select 
sellerid
from ham_jituan.dwd_client_event_gio_di  
where dt = '2023-07-11' 
and hour = '11'
and sellerid is not null
--and instr(attributes, 'WT_ets') > 0
limit 1;



WT_branch


select 
attributes
from ham.dwd_dcslog_event_gio_di
where dt = '2023-07-13' 
--and hour = '11'
and instr(attributes, 'event_type') > 0
limit 1;