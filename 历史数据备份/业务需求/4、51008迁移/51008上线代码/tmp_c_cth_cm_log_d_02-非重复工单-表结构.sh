

#表---tmp_c_cth_cm_log_d_02_local---重复工单表
#字段
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
login_type String, 
is_webtends String, 
if_safe_audit String, 
editor String, 
editor_phone_num String, 
pro_company String, 
template_id String, 
marketing_type String, 
wt_ac_id String, 
conf_chan String, 
area_type String


serial_id,start_time,down_time,status,issue_range,if_own_develop,area_name,area_location,issue_id,marke_name,url,editor_provname,frequency,marketing_mode,if_prov_page,marketing_id,icon_code,login_type,is_webtends,if_safe_audit,editor,editor_phone_num,pro_company,template_id,marketing_type,wt_ac_id,conf_chan,area_type




#分布式表——本地
CREATE TABLE ham.tmp_c_cth_cm_log_d_02_local on cluster cluster_gio_with_shard
(
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
login_type String, 
is_webtends String, 
if_safe_audit String, 
editor String, 
editor_phone_num String, 
pro_company String, 
template_id String, 
marketing_type String, 
wt_ac_id String, 
conf_chan String, 
area_type String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.tmp_c_cth_cm_log_d_02_local', '{replica}')
ORDER BY (marketing_id,frequency,icon_code)
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.tmp_c_cth_cm_log_d_02_all on cluster cluster_gio_with_shard
as ham.tmp_c_cth_cm_log_d_02_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'tmp_c_cth_cm_log_d_02_local', rand());

