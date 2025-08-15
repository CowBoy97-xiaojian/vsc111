select  dt,
	if(page_id is null, '', page_id) AS page_id,
	substr(if((channel_id like'C%' or channel_id like'P%'),substr(channel_id,0,12),channel_id),0,63),
        split(regexp_replace(url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '__'), '&')[0] AS link,
	count(ck_id) AS pv,
	count(distinct(ck_id)) AS uv 
from 
	dwd_dcslog_event_di 
where dt='${DT}' and url not like '%-https%'
group by dt,page_id,if((channel_id like'C%' or channel_id like'P%'),substr(channel_id,0,12),channel_id),url having(pv>0 or uv>0);