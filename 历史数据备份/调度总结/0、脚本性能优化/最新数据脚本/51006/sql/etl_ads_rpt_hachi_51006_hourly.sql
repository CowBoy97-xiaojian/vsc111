set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

INSERT OVERWRITE TABLE ham.ads_rpt_hachi_51006_hourly partition (dt = '${DT}', hour = '${HH}')
SELECT  daytime
       ,ip
       ,channel_id
       ,page_id
       ,seller_id
       ,wtdcsdat
       ,wtchannelid
       ,wtco_f
       ,wtevent
       ,wturl
       ,wtmc_ev
       ,wtsellerid
       ,wtpageid
       ,wtlast_pageid
       ,regexp_replace(wtlast_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')
       ,wtlast_pagename
       ,wtcurrent_url
       ,wtcurrent_pagename
       ,wtmodule_no
       ,wtmodule_name
       ,wtpoint_position
       ,wtmember
       ,wtgoods_no
       ,wtlogin_status
       ,wtnext_pageid
       ,wtnext_url
       ,wtnext_pagename
       ,wtcomponent_id
       ,user_agent
       ,referer
       ,mobile
       ,session_id
       ,wtinput_sfz
       ,environment
       ,ip_prov
       ,plat
       ,si_x
       ,si_n
       ,si_s
       ,goods_id
       ,sku_id
       ,prepare1
       ,event_type
       ,envname
       ,errcode
       ,errmsg
       ,prov
       ,city
       ,mc_ev_name
       ,trmnl_style
       ,tbd
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
       ,ip_city
       ,wt_input_mobile
       ,wt_goods_specs
       ,wt_goods_price
FROM ham.ads_rpt_hachi_51006_dt_hour
WHERE dt = '${DT}'
and hour = '${HH}'
and trmnl_style in ('H5','小程序','公众号','2r9o','3l2h','4t5g','9o4w')
;
