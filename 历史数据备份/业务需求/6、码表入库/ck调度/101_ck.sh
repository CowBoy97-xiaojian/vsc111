#! /bin/bash

DT=$1
hour=$2

clickhouse-client -h 10.253.248.73 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="
select dt, province, event1, page_id, channel_id, count(distinct ck_id) as uv
from (select dt,
             concat(ifnull(splitByString('_',ifnull(event,''))[3],''),'_',ifnull(splitByString('_',ifnull(event,''))[5],''),'_click') as event1,
             if((b.code is null), '-2', b.code)                                as province,
             page_id,
             substr(if((channel_id like'C%' or channel_id like'P%'),substr(channel_id,1,13),channel_id),1,64) as channel_id,
             ck_id
      from webtrends.event_hi_dcslog_all a
               left join ham.dim_province_code_all b on a.ip_prov = b.province
      where dt = '2023-04-10'
        and dcsid = 'dcscx966fo4l7j258ag0s874n_3w7x'
        and match(ifnull(event,''),'20200423BZK_JHY_(tab|ljlq)_[0-9]+_[0-9]+')) as all
group by event1, dt, province, page_id, channel_id having uv > 0
FORMAT CSV
"

