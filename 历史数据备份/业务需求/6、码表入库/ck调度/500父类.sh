
SELECT tbb.id,
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
   FROM webtrends.event_hi_dcslog_all
   WHERE 1=1
     AND dt= '2023-04-07'
   GROUP BY event,
            page_id,
            channel_id) AS tba
INNER JOIN ham.dim_dcslog_event_busi_all tbb ON tba.event = tbb.event
LEFT JOIN (select * from ham.dim_order_channel_all  where create_date = toString(toYYYYMMDD(addDays(toDate('2023-04-07'), -1)))) tbc ON tba.channel_id = tbc.channel_id_l4;