#! /bin/bash

DT=$1
hour=$2

clickhouse-client -h 10.253.248.73 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="
select  dt,
	if((page_id is null or page_id = '--'), '', page_id) AS page_id,
	substr(if((channel_id like 'C%' or channel_id like 'P%'),substr(channel_id,1,13),channel_id),1,64),
	splitByString('&',replaceRegexpAll(ifnull(url,''), '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '__'))[1] as link,
	count(ck_id) AS pv,
	count(distinct(ck_id)) AS uv 
from 
	webtrends.event_hi_dcslog_all 
where dt='2023-04-07' and url not like '%-https%'
group by dt,page_id,if((channel_id like 'C%' or channel_id like 'P%'),substr(channel_id,1,13),channel_id),url having(pv>0 or uv>0)
FORMAT CSV
"
