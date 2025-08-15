SELECT dt,
       tr_id,
       page_id,
       if((channel_id like'C%' or channel_id like'P%'),substr(channel_id,0,12),channel_id),
       busi_name,
       page_name,
       event_name,
       event,
       is_homepage,
       page_id,
       pv,
       uv
FROM ads_rpt_ev_ch_tr_di WHERE dt = '${DT}'
      order by dt,tr_id,event,page_id,if((channel_id like'C%' or channel_id like'P%'),substr(channel_id,0,12),channel_id);