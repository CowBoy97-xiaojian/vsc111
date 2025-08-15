#!/bin/bash

DTF=$1
DT=$(date -d $DTF +"%Y-%m-%d")

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="truncate table  ham.tmp_c_cth_cm_log_d_01_local on cluster cluster_gio_with_shard"


clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="
insert into table ham.tmp_c_cth_cm_log_d_01_all
select serial_id           
,start_time          
,down_time           
,status              
,issue_range         
,if_own_develop      
,area_name           
,area_location       
,issue_id            
,marke_name          
,url                 
,editor_provname     
,a.frequency           
,marketing_mode      
,if_prov_page        
,a.marketing_id
,icon_code            
,login_type          
,is_webtends         
,if_safe_audit       
,editor              
,editor_phone_num    
,pro_company         
,template_id         
,marketing_type      
,wt_ac_id   
,conf_chan
,area_type
from(select serial_id           
,start_time          
,down_time           
,status              
,issue_range         
,if_own_develop      
,area_name       
,area_location 
,issue_id 
,marke_name 
,url 
,editor_provname
,frequency  
,marketing_mode
,if_prov_page
,case when match(marketing_id,'_') > 0 then splitByChar('_',marketing_id)[1] else marketing_id end as marketing_id  -- 20210916修改，增加历史数据兼容
,icon_code
,login_type
,is_webtends         
,if_safe_audit
,editor
,editor_phone_num
,pro_company
,template_id
,marketing_type
,wt_ac_id
,conf_chan
,area_type
from ham.dim_client_ord_info_d_all 
where dt='${DT}') a
inner join(select frequency,marketing_id,count(1) as pv 
from(select start_time, down_time, editor_provname, 
area_name, area_location, marke_name, url,
frequency, marketing_mode, 
case when match(marketing_id,'_') > 0 then splitByChar('_',marketing_id)[1] else marketing_id end as marketing_id  -- 20210916修改，增加历史数据兼容
,dt,count(1) as pv 
from ham.dim_client_ord_info_d_all 
where dt = '${DT}'
group by start_time, down_time, editor_provname, area_name, area_location, marke_name, url,frequency, marketing_mode, marketing_id, dt) a           
group by frequency
,marketing_id 
having pv > 1) b --判断重复频道，如果有 增加匹配条件
on a.frequency=b.frequency
and a.marketing_id=b.marketing_id
;"