select
dt,
count(distinct case when query['WT.event'] = '20200917_page' 
and regexp_replace(query['WT.login_status'],'\\|','_') = '1' 
then mobile end) as dl_yh
from ods_dcslog_delta
where dt > '2023-07-20'
and dcsid = '3l2h'
and regexp_replace(query['WT.current_url'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','_') in ('ydqnhyb','ydqnhybwxxx','ydqnhybms','ypyhg','ydqnhyb728yr')
group by dt order by dt;


select
dt,count(1)
from ods_dcslog_delta 
where dt > '2023-07-20'
and dcsid = '3l2h'
group by dt order by dt;

select
dt,
count(1) as pv
from ods_dcslog_delta
where dt > '2023-07-20'
and dcsid = '3l2h'
and regexp_replace(query['WT.login_status'],'\\|','_') = '1' 
and regexp_replace(query['WT.current_url'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','_') in ('ydqnhyb','ydqnhybwxxx','ydqnhybms','ypyhg','ydqnhyb728yr')
group by dt order by dt;