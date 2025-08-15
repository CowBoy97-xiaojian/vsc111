SET hive.exec.compress.output = true;
SET mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.GzipCodec;
INSERT OVERWRITE TABLE ham_jituan.ads_hachi_cm002 partition (prov = '${PROV}')
SELECT  date_time
       ,c_ip
       ,cs_host
       ,wt_cid
       ,wt_co_f
       ,wt_city
       ,wt_mobile
       ,wt_event
       ,wt_ti
       ,regexp_replace(wt_es, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')
       ,mark_id
       ,trmnl_style
       ,wt_av
       ,date_day
       ,click_time
       ,wt_prov
       ,si_x
       ,si_n
       ,si_s
       ,wt_goods_id
       ,wt_sku_id
       ,wt_channel_id
       ,user_agent
       ,os
       ,dm
       ,sv
       ,a_dc
       ,ct
       ,sr
       ,vt_sid
       ,userbrand
       ,loginprovince
       ,logincity
       ,appid
       ,channel
       ,dcsref
       ,et
       ,ed
       ,envname
       ,next_url
       ,errcode
       ,errmsg
       ,serial_no
       ,act_str_step_id
       ,group_id
       ,mr_id
       ,ad_step
       ,touch_tm
       ,appqry
       ,xy
       ,sid
FROM ham_jituan.dwd_client_event_di
WHERE dt = '${DT}'
AND wt_prov = '${PROV_ID}'
AND trmnl_style='MPX_App'
;

