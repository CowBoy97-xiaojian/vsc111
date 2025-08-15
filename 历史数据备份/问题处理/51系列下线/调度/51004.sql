SELECT row_number() over(),
                    dt,
                    page_id,
                    channel_id,
                    channel_name,
                    channel_type,
                    ip_prov,
                    pv,
                    uv, 
                    url
FROM ads_rpt_ch_tr_di
WHERE 1=1
  AND dt= '${DT}';

INSERT overwrite TABLE ads_rpt_ch_tr_di partition(dt= '${DT}')
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
     AND dt= '${DT}'
   GROUP BY url,
            channel_id,
            ip_prov,
            page_id) AS tba
LEFT JOIN (select * from dim_order_channel  where dt=regexp_replace(date_add('${DT}',-1),'-','')) tbc ON tba.channel_id = tbc.channel_id_l4;