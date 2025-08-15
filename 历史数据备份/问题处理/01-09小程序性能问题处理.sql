select * 
from olap.event_all
where 
toDate(event_time) = '2024-01-07'
and data_source_id = 'a1f48d9ff4f42571'
and user in ('vPMCxVWYdR6DYT+SVxVVig==','15951241207')
and event_key = 'pfm'
and attributes['WT_event'] ='c_perf'
limit 1
format CSVWithNames;