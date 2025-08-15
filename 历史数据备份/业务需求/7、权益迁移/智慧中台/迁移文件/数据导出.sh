#! /bin/bash

DT=$1
hour=$2

clickhouse-client -h 10.253.248.73 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="
select 
rowNumberInAllBlocks()+1
date, 
untv_scene_code, 
untv_abl_code, 
untv_service_code, 
abl_user_name, 
abl_user_code, 
abl_user_applname, 
abl_user_applcode, 
abl_user_orgname, 
abl_user_orgcode, 
portal_no, 
portal_sub_no, 
invoke_amt, 
invoke_fail_rsn
from ham.ads_zhzt_amt_di_all
where dt = '${DT}'
FORMAT CSV
"