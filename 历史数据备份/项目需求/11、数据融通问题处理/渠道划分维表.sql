

CREATE TABLE `dim_rpt_hachi_domain`(
  `domain` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `interface` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='#_#') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/dim_rpt_hachi_domain'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'last_modified_by'='udbac', 
  'last_modified_time'='1689131799', 
  'numFiles'='23', 
  'numRows'='67598281', 
  'rawDataSize'='0', 
  'totalSize'='2697495630', 
  'transient_lastDdlTime'='1689131799')