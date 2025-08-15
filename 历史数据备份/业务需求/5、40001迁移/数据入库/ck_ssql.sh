#!/bin/bash

DTF=$1
DT=$(date -d $DTF +"%Y-%m-%d")

#先删除数据
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr


"

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="
insert into table ham.ads_rpt_hachi_40001_all
select
daytime
,ip
,channel_id
,page_id
,seller_id
,dcsdat
,wtchannelid
,ck_id
,event
,es
,mc_ev
,seller_id
,page_id
,last_pageid
,last_url
,last_pagename
,current_url
,current_pagename
,module_no
,module_name
,point_position
,member
,goods_no
,login_status
,next_pageid
,next_url
,next_pagename
,component_id
,user_agent
,referer
,mobile
,ss_id
,input_sfz
,environment
,ip_prov
,code 
from webtrends.event_hi_dcslog_all tb1 inner join ham.dim_prov_code_all tb2 on tb1.ip_city=tb2.ip_prov
where dt='2023-04-07' 
  and mc_ev='210315_QYCS'
  and daytime is not null;"
