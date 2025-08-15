SELECT row_number() over(),
                    dt,
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
FROM ads_rpt_ev_ch_tr_pr_di
WHERE 1=1
  AND dt= '${DT}';


INSERT overwrite TABLE ads_rpt_ev_ch_tr_pr_di partition(dt= '${DT}')
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
     AND dt= '${DT}'
   GROUP BY event,
            channel_id,
            ip_prov,
            page_id) AS tba
INNER JOIN dim_dcslog_event_busi tbb ON tba.event = tbb.event
LEFT JOIN (select * from dim_order_channel  where dt=regexp_replace(date_add('${DT}',-1),'-','')) tbc ON tba.channel_id = tbc.channel_id_l4 ;