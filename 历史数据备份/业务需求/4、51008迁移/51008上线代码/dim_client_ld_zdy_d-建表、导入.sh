#ck库入库语法
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' 
--query="truncate table ham.dim_order_detail_all" < 0002_00100_20230326.txt

#!/bin/bash

DTF=$1
DT=$(date -d $DTF +"%Y-%m-%d")

cd /home/udbac/input/ld_workorder_ck

#1、替换字符
cat a_ld_${DTF}_JZYY_XTB2_55202_0?_001.dat | sed "s/€€/\\|/g" > a_ld_${DTF}_JZYY_XTB2_55202_0?_001_sed.dat

#2、清空tmp表
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="truncate table ham.dim_client_ld_zdy_d_tmp"

#3、入tmp表
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="insert into ham.dim_client_ld_zdy_d_tmp FORMAT CSV" < a_ld_${DTF}_JZYY_XTB2_55202_0?_001_sed.dat

#4、删除分区--重跑
	clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="alter table ham.dim_client_ld_zdy_d_local on cluster cluster_gio_with_shard drop partition '${DT}'"

#5、导入数据
	clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="insert into ham.dim_client_ld_zdy_d_all(record_num,advertype,mark_id,prov_name,frequency,area_name,url,event,marke_name,develop_type,remarks,create_time,create_person,dt) SELECT record_num,advertype,mark_id,prov_name,frequency,area_name,url,event,marke_name,develop_type,remarks,create_time,create_person,'${DT}s' FROM ham.dim_client_ld_zdy_d_tmp;"	



#hive库字段
  record_num    String, 
  advertype     String, 
  mark_id       String, 
  prov_name     String, 
  frequency     String, 
  area_name     String, 
  url           String, 
  event         String, 
  marke_name    String, 
  develop_type  String, 
  remarks       String, 
  create_time   String, 
  create_person String

record_num,advertype,mark_id,prov_name,frequency,area_name,url,event,marke_name,develop_type,remarks,create_time,create_person,dt

#临时表
CREATE TABLE ham.dim_client_ld_zdy_d_tmp
(
    record_num    String, 
    advertype     String, 
    mark_id       String, 
    prov_name     String, 
    frequency     String, 
    area_name     String, 
    url           String, 
    event         String, 
    marke_name    String, 
    develop_type  String, 
    remarks       String, 
    create_time   String, 
    create_person String
)   
ENGINE = MergeTree
ORDER BY mark_id
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';



#分布式表——本地
CREATE TABLE ham.dim_client_ld_zdy_d_local on cluster cluster_gio_with_shard
(
    record_num    String, 
    advertype     String, 
    mark_id       String, 
    prov_name     String, 
    frequency     String, 
    area_name     String, 
    url           String, 
    event         String, 
    marke_name    String, 
    develop_type  String, 
    remarks       String, 
    create_time   String, 
    create_person String,
    dt            String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_client_ld_zdy_d_local', '{replica}')
PARTITION BY dt
ORDER BY (dt,event,mark_id)
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.dim_client_ld_zdy_d_all on cluster cluster_gio_with_shard
as ham.dim_client_ld_zdy_d_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_client_ld_zdy_d_local', rand());