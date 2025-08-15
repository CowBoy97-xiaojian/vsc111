#! /bin/bash

DT=$1

clickhouse-client -h 10.253.248.73 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="
select 
rowNumberInAllBlocks()+1
,daytime
,ip
,cli_time
,channel_id
,ck_id
,vt_sid
,event
,es
,mc_ev
,next_url
,user_agent
,mobile
,si_x
,si_n
,si_s
,goods_id
,sku_id
,envname
,errcode
,errmsg
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
from webtrends.event_hi_dcslog_all
where dt = '${DT}'
and dcsid in ('3s3i','7p1j','3a5k','8z2s','1w7q','9w6z','8f4q','2i1b','6g7b','0d4d','8d2o','8m4a','5d3k','3z9h','4w6p','5f8e','3j1d','9o0p','0o1j','3z7i','9b5g','3r8r','3d2m','9m2x','8o8q','2v6r','8j5v','4z6i','9y7c','8s0b','1g3e')
limit 100;
FORMAT CSV
"
