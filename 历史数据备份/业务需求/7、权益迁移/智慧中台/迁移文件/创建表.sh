数据中台一建表语句
CREATE TABLE `ham_jituan.ads_zhzt_amt_di`(
  `date` string COMMENT 'from deserializer', 
  `untv_scene_code` string COMMENT 'from deserializer', 
  `untv_abl_code` string COMMENT 'from deserializer', 
  `untv_service_code` string COMMENT 'from deserializer', 
  `abl_user_name` string COMMENT 'from deserializer', 
  `abl_user_code` string COMMENT 'from deserializer', 
  `abl_user_applname` string COMMENT 'from deserializer', 
  `abl_user_applcode` string COMMENT 'from deserializer', 
  `abl_user_orgname` string COMMENT 'from deserializer', 
  `abl_user_orgcode` string COMMENT 'from deserializer', 
  `portal_no` string COMMENT 'from deserializer', 
  `portal_sub_no` string COMMENT 'from deserializer', 
  `invoke_amt` string COMMENT 'from deserializer', 
  `invoke_fail_rsn` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='#_#', 
  'serialization.null.format'='--') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham_jituan.db/ads_zhzt_amt_di'
TBLPROPERTIES (
  'transient_lastDdlTime'='1674975407')



  #分布式表——本地
CREATE TABLE ham.ads_zhzt_amt_di_local on cluster cluster_gio_with_shard
(
    date Nullable(String), 
    untv_scene_code Nullable(String), 
    untv_abl_code Nullable(String), 
    untv_service_code Nullable(String), 
    abl_user_name Nullable(String), 
    abl_user_code Nullable(String), 
    abl_user_applname Nullable(String), 
    abl_user_applcode Nullable(String), 
    abl_user_orgname Nullable(String), 
    abl_user_orgcode Nullable(String), 
    portal_no Nullable(String), 
    portal_sub_no Nullable(String), 
    invoke_amt Nullable(String), 
    invoke_fail_rsn Nullable(String),
    dt String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.ads_zhzt_amt_di_local', '{replica}')
PARTITION BY dt
ORDER BY dt
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.ads_zhzt_amt_di_all on cluster cluster_gio_with_shard
as ham.ads_zhzt_amt_di_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'ads_zhzt_amt_di_local', rand());