select 
count(1)
from  olap.event_all
where
data_source_id='a1f48d9ff4f42571'  
and  event_time >= '1707118055245' and event_time <= '1707118248372'
--and  user = 'iwmEDbUJajKEVKvan9FdVw=='
and attributes['WT_et']='pfm'
and attributes['WT_event'] in ('pfm_mpxdnloaded','pfm_mpxed','pfm_mpxloaded','pfm_mpxdnloadedsum')
limit 6;
1707118055245
1707118248372

 toDate(event_time)='2024-01-30' and 
LPAD(toString(toHour(event_time)), 2, '0')='16'
--and event_time='1706601788000'
--and session='5360cbb0-3e20-4323-b346-7cb8b7bcf75d'
and  user = 'iwmEDbUJajKEVKvan9FdVw=='
and attributes['WT_event'] in ('pfm_mpxdnloaded','pfm_mpxed','pfm_mpxloaded','pfm_mpxdnloadedsum')
and attributes['WT_et']='pfm'
and  data_source_id='a1f48d9ff4f42571'
format CSV;