--中间表
CREATE TABLE `ods_zhzt_jf_dim`(
  `prov` string, 
  `flow_code_sub` string, 
  `ability_code` string, 
  `service_code` string, 
  `order_code` string, 
  `sub_order_code` string, 
  `charge_id` string, 
  `charge_rule_id` string, 
  `company` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham_jituan.db/ods_zhzt_jf_dim'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='14', 
  'numRows'='14', 
  'rawDataSize'='1260', 
  'totalSize'='1274', 
  'transient_lastDdlTime'='1679046108')



  --日表
createtab_stmt
CREATE TABLE `ods_zhzt_jf_di`(
  `num` int COMMENT 'from deserializer', 
  `flow_code` string COMMENT 'from deserializer', 
  `ability_code` string COMMENT 'from deserializer', 
  `service_code` string COMMENT 'from deserializer', 
  `order_code` string COMMENT 'from deserializer', 
  `sub_order_code` string COMMENT 'from deserializer', 
  `charge_id` string COMMENT 'from deserializer', 
  `charge_rule_id` string COMMENT 'from deserializer', 
  `company` string COMMENT 'from deserializer', 
  `count_by_prvo` string COMMENT 'from deserializer', 
  `reserve_1` string COMMENT 'from deserializer', 
  `reserve_2` string COMMENT 'from deserializer', 
  `reserve_3` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer')
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
  'hdfs://udbachdp1/user/hive/warehouse/ham_jituan.db/ods_zhzt_jf_di'
TBLPROPERTIES (
  'transient_lastDdlTime'='1679044017')



  --月表

createtab_stmt
CREATE TABLE `ods_zhzt_jf_dm`(
  `num` int COMMENT 'from deserializer', 
  `ability_code` string COMMENT 'from deserializer', 
  `service_code` string COMMENT 'from deserializer', 
  `order_code` string COMMENT 'from deserializer', 
  `sub_order_code` string COMMENT 'from deserializer', 
  `charge_id` string COMMENT 'from deserializer', 
  `charge_rule_id` string COMMENT 'from deserializer', 
  `company` string COMMENT 'from deserializer', 
  `count_by_prvo` string COMMENT 'from deserializer', 
  `reserve_1` string COMMENT 'from deserializer', 
  `reserve_2` string COMMENT 'from deserializer', 
  `reserve_3` string COMMENT 'from deserializer', 
  `month_time` string COMMENT 'from deserializer')
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
  'hdfs://udbachdp1/user/hive/warehouse/ham_jituan.db/ods_zhzt_jf_dm'
TBLPROPERTIES (
  'transient_lastDdlTime'='1679046213')