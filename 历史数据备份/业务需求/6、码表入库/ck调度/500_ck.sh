#! /bin/bash

DT=$1
hour=$2

clickhouse-client -h 10.253.248.73 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="
SELECT '2023-04-10',
       tr_id,
       page_id,
       if((channel_id like'C%' or channel_id like'P%'),substr(channel_id,1,13),channel_id),
       busi_name,
       page_name,
       event_name,
       event,
       is_homepage,
       page_id,
       pv,
       uv
FROM 
(
      select
       tbb.id as tr_id,
       tbb.busi_name as busi_name,
       tbb.page_name as page_name,
       tbb.event_name as event_name,
       tba.event as event,
       tbb.is_homepage as is_homepage,
       tba.channel_id as channel_id,
       tbc.channel_name_l4 as channel_name,
       tbc.channel_type as channel_type,
       tba.page_id as page_id,
       '' as mark2,
       '' as mark3,
       '' as mark4,
       '' as mark5,
       pv,
       uv
FROM
  (SELECT event,
          channel_id,
          page_id,
          count(1) AS pv,
          count(DISTINCT ck_id) AS uv
   FROM webtrends.event_hi_dcslog_all
   WHERE 1=1
     AND dt= '2023-04-10'
   GROUP BY event,
            page_id,
            channel_id) AS tba
INNER JOIN ham.dim_dcslog_event_busi_all tbb ON tba.event = tbb.event
LEFT JOIN (select * from ham.dim_order_channel_all  where create_date = toString(toYYYYMMDD(addDays(toDate('2023-04-10'), -1)))) tbc ON tba.channel_id = tbc.channel_id_l4
)
order by tr_id,event,page_id,if((channel_id like'C%' or channel_id like'P%'),substr(channel_id,1,13),channel_id)
FORMAT CSV
"