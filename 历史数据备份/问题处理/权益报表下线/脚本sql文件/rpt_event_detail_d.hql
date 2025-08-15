SELECT dt,
       tr_id,
       busi_name,
       page_name,
       event_name,
       event,
       is_homepage,
       channel_id,
       channel_name,
       channel_type,
       page_id,
       pv,
       uv
FROM ads_rpt_ev_ch_tr_di WHERE dt = '${DT}'
      order by dt,tr_id,event,page_id,channel_id
;
