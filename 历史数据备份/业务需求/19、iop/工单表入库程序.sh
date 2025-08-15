#!/bin/bash

DTF=$1
DT=$(date -d $DTF +"%Y-%m-%d")

cd /home/udbac/input/ld_workorder_ck

#1、替换字符
cat a_ld_${DTF}_JZYY_XTB2_55201_0?_001.dat | sed "s/€€/\x01/g" | sed "s/\\\"/'/g" > a_ld_${DTF}_JZYY_XTB2_55201_001_sed.dat

#2、清空tmp表
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="truncate table ham.dim_client_work_order_d_tmp"

#3、入tmp表
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="insert into ham.dim_client_work_order_d_tmp FORMAT CSV" < a_ld_${DTF}_JZYY_XTB2_55201_001_sed.dat

#4、删除分区--重跑
        clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="alter table ham.dim_client_work_order_d_local on cluster cluster_gio_with_shard drop partition '${DT}'"

#5、导入数据
        clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="insert into ham.dim_client_work_order_d_all(record_num,serial_id,start_time,down_time,status,issue_range,if_own_develop,area_name,area_location,issue_id,marke_name,url,editor_provname,frequency,marketing_mode,if_prov_page,marketing_id,icon_code,iop_sub_activity_id,iop_operation_id,promotional_content,contacts_id,page_id,login_type,is_webtends,if_safe_audit,editor,editor_phone_num,pro_company,template_id,marketing_type,wt_ac_id,sub_title,postage_type,tariff_code,boss_id,dt) SELECT record_num,serial_id,start_time,down_time,status,issue_range,if_own_develop,area_name,area_location,issue_id,marke_name,url,editor_provname,frequency,marketing_mode,if_prov_page,marketing_id,icon_code,iop_sub_activity_id,iop_operation_id,promotional_content,contacts_id,page_id,login_type,is_webtends,if_safe_audit,editor,editor_phone_num,pro_company,template_id,marketing_type,wt_ac_id,sub_title,postage_type,tariff_code,boss_id,'${DT}' FROM ham.dim_client_work_order_d_tmp"