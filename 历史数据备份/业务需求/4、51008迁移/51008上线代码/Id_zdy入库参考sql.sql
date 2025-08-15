ham_jituan.dim_client_ld_zdy_d
CREATE EXTERNAL TABLE `ham_jituan.dim_client_ld_zdy_d`(
  `record_num` string COMMENT 'from deserializer', 
  `advertype` string COMMENT 'from deserializer', 
  `mark_id` string COMMENT 'from deserializer', 
  `prov_name` string COMMENT 'from deserializer', 
  `frequency` string COMMENT 'from deserializer', 
  `area_name` string COMMENT 'from deserializer', 
  `url` string COMMENT 'from deserializer', 
  `event` string COMMENT 'from deserializer', 
  `marke_name` string COMMENT 'from deserializer', 
  `develop_type` string COMMENT 'from deserializer', 
  `remarks` string COMMENT 'from deserializer', 
  `create_time` string COMMENT 'from deserializer', 
  `create_person` string COMMENT 'from deserializer')
COMMENT 'created by dacp create table function'
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u20AC\u20AC') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham_jituan.db/dim_client_ld_zdy_d'
TBLPROPERTIES (
  'transient_lastDdlTime'='1639982793')
