INSERT overwrite TABLE ads_rpt_ev_ch_tr_di partition(dt= '${DT}')
SELECT tbb.`id`,
       tbb.busi_name,
       tbb.page_name,
       tbb.event_name,
       tba.event,
       tbb.is_homepage,
       tba.channel_id,
       tbc.channel_name_l4,
       tbc.channel_type,
       tba.page_id,
       '',
       '',
       '',
       '',
       pv,
       uv
FROM
  (SELECT event,
          channel_id,
          page_id,
          count(1) AS pv,
          count(DISTINCT ck_id) AS uv
   FROM dwd_dcslog_event_di
   WHERE 1=1
     AND dt= '${DT}'
   GROUP BY event,
            page_id,
            channel_id) AS tba
INNER JOIN dim_dcslog_event_busi tbb ON tba.event = tbb.event
LEFT JOIN (select * from dim_order_channel  where dt=regexp_replace(date_add('${DT}',-1),'-','')) tbc ON tba.channel_id = tbc.channel_id_l4;
