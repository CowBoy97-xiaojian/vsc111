INSERT overwrite TABLE ads_rpt_ev_ch_tr_hi partition(dt= '${DT}', hour='${HH}')
SELECT tbb.`id`,
       tbb.busi_name,
       tbb.page_name,
       tbb.event_name,
       tba.event,
       tbb.is_homepage,
       tba.channel_id,
       tbc.channel_name_l4,
       tbc.channel_type,
       page_id,
       tbb.url,
       tba.ip_prov,
       '',
       '',
       '',
       pv,
       uv
FROM
  (SELECT event,
          channel_id,
          page_id,
          ip_prov,
          count(1) AS pv,
          count(DISTINCT ck_id) AS uv
   FROM dwd_dcslog_event_di
   WHERE 1=1
     AND dt= '${DT}' AND hour='${HH}'
   GROUP BY event,
            channel_id,
            ip_prov,
            page_id) AS tba
INNER JOIN dim_dcslog_event_busi tbb ON tba.event = tbb.event
LEFT JOIN (select * from dim_order_channel  where dt=regexp_replace(date_add('${DT}',-1),'-','')) tbc ON tba.channel_id = tbc.channel_id_l4;



INSERT overwrite TABLE ads_rpt_ch_tr_hi partition(dt= '${DT}', hour='${HH}')
SELECT tba.channel_id,
       tbc.channel_name_l4,
       tbc.channel_type,
       page_id,
       tba.url,
       tba.ip_prov,
       '',
       '',
       '',
       pv,
       uv
FROM
  (SELECT channel_id,
          page_id,
          ip_prov,
          url,
          count(1) AS pv,
          count(DISTINCT ck_id) AS uv
   FROM dwd_dcslog_event_di
   WHERE 1=1
     AND dt= '${DT}' AND hour='${HH}'
   GROUP BY url,
            channel_id,
            ip_prov,
            page_id) AS tba
LEFT JOIN (select * from dim_order_channel  where dt=regexp_replace(date_add('${DT}',-1),'-','')) tbc ON tba.channel_id = tbc.channel_id_l4 ;



--51001
SELECT row_number() over(),
                    dt,
                    hour,
                    busi_name,
                    page_name,
                    event_name,
                    event,
                    is_homepage,
                    page_id,
                    channel_id,
                    channel_name,
                    channel_type,
                    pv,
                    uv, 
                    ip_prov
FROM ads_rpt_ev_ch_tr_hi
WHERE 1=1
  AND dt= '${DT}'
  AND hour = '${HH}';

--51002
SELECT row_number() over(),
                    dt,
                    hour,
                    page_id,
                    channel_id,
                    channel_name,
                    channel_type,
                    ip_prov,
                    pv,
                    uv, 
                    url
FROM ads_rpt_ch_tr_hi
WHERE 1=1
  AND dt= '${DT}'
  AND hour = '${HH}';


select wt_channelid,count(1) as pv,count(DISTINCT ck_id) from event_hi_dcslog_51001_all where dt = '2024-01-18' and hour = '15'  group by wt_channelid;

select dt,hour,count(1) from  webtrends.event_hi_dcslog_51001_all group by dt,hour;


select 
wt_channelid,
count(1) as pv,
count(DISTINCT ck_id) as uv,
statis_hour
from event_hi_dcslog_51001_all 
where dt = '2024-01-29' and hour = '16'  group by wt_channelid,statis_hour limit 50;


a_10000_2024011909_cm_csap_77029_00_001.dat
a_10000_2024011909_cm_csap_77029_00.verf

a_10000_2024011909_cm_csap_77029_00_001.dat
a_10000_2024011909_cm_csap_77029_00.verf