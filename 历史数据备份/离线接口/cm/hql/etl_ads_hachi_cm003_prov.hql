set hive.vectorized.execution.enabled  = false;
SET hive.exec.compress.output = true;
SET mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.GzipCodec;
INSERT OVERWRITE TABLE ham.ads_hachi_cm003 partition (prov = '${PROV}')
SELECT  daytime
       ,ip
       ,dcsdat
       ,channel_id
       ,ck_id
       ,vt_sid
       ,event
       ,regexp_replace(es, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')       
       ,mc_ev
       ,next_url
       ,user_agent
       ,mobile
       ,regexp_replace(si_x, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')
       ,regexp_replace(si_n, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')
       ,regexp_replace(si_s, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')
       ,goods_id
       ,sku_id
       ,regexp_replace(envname, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')
       ,regexp_replace(errcode, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')
       ,regexp_replace(errmsg, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')
       ,prov
       ,city
       ,trmnl_style
       ,sr
       ,appid
       ,cid
       ,userbrand
       ,loginprovince
       ,logincity
       ,nodeid
       ,tv
       ,vt_f_tlh
       ,vtvs
       ,branch
       ,dcsuri
       ,ti
       ,et
       ,markid
       ,serial_no
       ,act_str_step_id
       ,group_id
       ,mr_id
       ,ad_step
       ,touch_tm
       ,ed
       ,xy
       ,prepare1
FROM ham.dwd_dcslog_event_di
WHERE dt = '${DT}'
AND dcsid = '${DCSID}'

