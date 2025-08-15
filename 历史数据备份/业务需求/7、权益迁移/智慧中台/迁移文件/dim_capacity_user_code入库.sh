CREATE EXTERNAL TABLE `dim_capacity_user_code`(
  `name` string COMMENT '机构名称', 
  `code` string COMMENT '机构编码', 
  `desc` string COMMENT '机构分类')
COMMENT '31省编码表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/dim_capacity_user_code'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'totalSize'='8988', 
  'transient_lastDdlTime'='1666338689')


  #分布式表——本地
CREATE TABLE ham.dim_capacity_user_code_local on cluster cluster_gio_with_shard
(
    name String,
    code String,
    desc String

)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_capacity_user_code_local', '{replica}')
ORDER BY code
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.dim_capacity_user_code_all on cluster cluster_gio_with_shard
as ham.dim_capacity_user_code_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_capacity_user_code_local', rand());


insert into table ham.dim_capacity_user_code_all on cluster values('北京公司','100','省公司'),('广东公司','200','省公司'),('上海公司','210','省公司'),('天津公司','220','省公司'),('重庆公司','230','省公司'),('辽宁公司','240','省公司'),('江苏公司','250','省公司'),('湖北公司','270','省公司'),('四川公司','280','省公司'),('陕西公司','290','省公司'),('河北公司','311','省公司'),('山西公司','351','省公司'),('河南公司','371','省公司'),('吉林公司','431','省公司'),('黑龙江公司','451','省公司'),('内蒙古公司','471','省公司'),('山东公司','531','省公司'),('安徽公司','551','省公司'),('浙江公司','571','省公司'),('福建公司','591','省公司'),('湖南公司','731','省公司'),('广西公司','771','省公司'),('江西公司','791','省公司'),('贵州公司','851','省公司'),('云南公司','871','省公司'),('西藏公司','891','省公司'),('海南公司','898','省公司'),('甘肃公司','931','省公司'),('宁夏公司','951','省公司'),('青海公司','971','省公司'),('新疆公司','991','省公司');