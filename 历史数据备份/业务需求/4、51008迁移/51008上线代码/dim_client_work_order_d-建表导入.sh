#ck库入库语法
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' 
--query="truncate table ham.dim_order_detail_all" < 0002_00100_20230326.txt

#!/bin/bash

DTF=$1
DT=$(date -d $DTF +"%Y-%m-%d")

cd /home/udbac/input/ld_workorder_ck

#1、替换字符
cat a_ld_${DTF}_JZYY_XTB2_55201_0?_001.dat | sed "s/€€/^/g" > a_ld_${DTF}_JZYY_XTB2_55201_0?_001_sed.dat

#2、清空tmp表
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="truncate table ham.dim_client_work_order_d_tmp"

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="DROP TABLE ham.dim_client_work_order_d_tmp"
DROP TABLE my_table;

#3、入tmp表
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --input_format_allow_errors_ratio=0.1 --query="insert into ham.dim_client_work_order_d_tmp FORMAT CSV" < a_ld_${DTF}_JZYY_XTB2_55201_0?_001_sed.dat

#4、删除分区--重跑
	clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="alter table ham.dim_client_work_order_d_local on cluster cluster_gio_with_shard drop partition '2023-04-06'"

#5、导入数据
	clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="insert into ham.dim_client_work_order_d_all(record_num,serial_id,start_time,down_time,status,issue_range,if_own_develop,area_name,area_location,issue_id,marke_name,url,editor_provname,frequency,marketing_mode,if_prov_page,marketing_id,icon_code,iop_sub_activity_id,iop_operation_id,promotional_content,contacts_id,page_id,login_type,is_webtends,if_safe_audit,editor,editor_phone_num,pro_company,template_id,marketing_type,wt_ac_id,sub_title,postage_type,tariff_code,boss_id,dt) SELECT record_num,serial_id,start_time,down_time,status,issue_range,if_own_develop,area_name,area_location,issue_id,marke_name,url,editor_provname,frequency,marketing_mode,if_prov_page,marketing_id,icon_code,iop_sub_activity_id,iop_operation_id,promotional_content,contacts_id,page_id,login_type,is_webtends,if_safe_audit,editor,editor_phone_num,pro_company,template_id,marketing_type,wt_ac_id,sub_title,postage_type,tariff_code,boss_id,'${DT}' FROM ham.dim_client_work_order_d_tmp"	



#hive库字段
record_num String, 
serial_id String, 
start_time String, 
down_time String, 
status String, 
issue_range String, 
if_own_develop String, 
area_name String, 
area_location String, 
issue_id String, 
marke_name String, 
url String, 
editor_provname String, 
frequency String, 
marketing_mode String, 
if_prov_page String, 
marketing_id String, 
icon_code String, 
iop_sub_activity_id String, 
iop_operation_id String, 
promotional_content String, 
contacts_id String, 
page_id String, 
login_type String, 
is_webtends String, 
if_safe_audit String, 
editor String, 
editor_phone_num String, 
pro_company String, 
template_id String, 
marketing_type String, 
wt_ac_id String, 
sub_title String, 
postage_type String, 
tariff_code String, 
boss_id String

record_num,serial_id,start_time,down_time,status,issue_range,if_own_develop,area_name,area_location,issue_id,marke_name,url,editor_provname,frequency,marketing_mode,if_prov_page,marketing_id,icon_code,iop_sub_activity_id,iop_operation_id,promotional_content,contacts_id,page_id,login_type,is_webtends,if_safe_audit,editor,editor_phone_num,pro_company,template_id,marketing_type,wt_ac_id,sub_title,postage_type,tariff_code,boss_id,dt

#临时表
CREATE TABLE ham.dim_client_work_order_d_tmp
(
record_num String, 
serial_id String, 
start_time String, 
down_time String, 
status String, 
issue_range String, 
if_own_develop String, 
area_name String, 
area_location String, 
issue_id String, 
marke_name String, 
url String, 
editor_provname String, 
frequency String, 
marketing_mode String, 
if_prov_page String, 
marketing_id String, 
icon_code String, 
iop_sub_activity_id String, 
iop_operation_id String, 
promotional_content String, 
contacts_id String, 
page_id String, 
login_type String, 
is_webtends String, 
if_safe_audit String, 
editor String, 
editor_phone_num String, 
pro_company String, 
template_id String, 
marketing_type String, 
wt_ac_id String, 
sub_title String, 
postage_type String, 
tariff_code String, 
boss_id String,
value1  String
)   
ENGINE = MergeTree
ORDER BY marketing_id
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';



#分布式表——本地
CREATE TABLE ham.dim_client_work_order_d_local on cluster cluster_gio_with_shard
(
record_num String, 
serial_id String, 
start_time String, 
down_time String, 
status String, 
issue_range String, 
if_own_develop String, 
area_name String, 
area_location String, 
issue_id String, 
marke_name String, 
url String, 
editor_provname String, 
frequency String, 
marketing_mode String, 
if_prov_page String, 
marketing_id String, 
icon_code String, 
iop_sub_activity_id String, 
iop_operation_id String, 
promotional_content String, 
contacts_id String, 
page_id String, 
login_type String, 
is_webtends String, 
if_safe_audit String, 
editor String, 
editor_phone_num String, 
pro_company String, 
template_id String, 
marketing_type String, 
wt_ac_id String, 
sub_title String, 
postage_type String, 
tariff_code String, 
boss_id String,
dt String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_client_work_order_d_local', '{replica}')
PARTITION BY dt
ORDER BY (dt,frequency,marketing_id,icon_code)
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.dim_client_work_order_d_all on cluster cluster_gio_with_shard
as ham.dim_client_work_order_d_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_client_work_order_d_local', rand());