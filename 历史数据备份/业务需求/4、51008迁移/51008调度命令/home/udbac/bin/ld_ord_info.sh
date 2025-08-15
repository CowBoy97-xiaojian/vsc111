#!/bin/bash

DTF=$1
DT=$(date -d $DTF +"%Y-%m-%d")
HS2='jdbc:hive2://master01:10000/ham'

hdfs dfs -rm -r /data/ld_zdy/$DT
hdfs dfs -mkdir -p /data/ld_zdy/$DT/
hdfs dfs -put /home/udbac/input/ld_workorder/a_ld_${DTF}_JZYY_XTB2_55202_0?_001.dat /data/ld_zdy/$DT/

hdfs dfs -rm -r /data/ld_workorder/$DT
hdfs dfs -mkdir -p /data/ld_workorder/$DT/
hdfs dfs -put /home/udbac/input/ld_workorder/a_ld_${DTF}_JZYY_XTB2_55201_0?_001.dat /data/ld_workorder/$DT/

dim_client_ld_zdy_d
dim_client_ord_info_d


beeline -n udbac -u $HS2 --hivevar DT=$DT -f /home/udbac/hqls/jituan/etl_dim_client_ld_zdy_d.hql
ALTER TABLE ham_jituan.dim_client_ld_zdy_d DROP partition(dt='${DT}');
ALTER TABLE ham_jituan.dim_client_ld_zdy_d ADD partition(dt='${DT}') LOCATION '/data/ld_zdy/${DT}';





beeline -n udbac -u $HS2 --hivevar DT=$DT -f /home/udbac/hqls/jituan/etl_dim_client_ord_info_d.hqls