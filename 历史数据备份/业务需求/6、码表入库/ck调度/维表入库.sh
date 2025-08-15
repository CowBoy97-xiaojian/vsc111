CREATE TABLE `dim_province_code`(
  `code` int, 
  `province` string)
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
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/dim_province_code'
TBLPROPERTIES (
  'transient_lastDdlTime'='1591711978')


  #分布式表——本地
CREATE TABLE ham.dim_province_code_local on cluster cluster_gio_with_shard
(
    code String,
    province String

)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_province_code_local', '{replica}')
ORDER BY province
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.dim_province_code_all on cluster cluster_gio_with_shard
as ham.dim_province_code_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_province_code_local', rand());

insert into table ham.dim_province_code_all values('-1','异网省'),('100','北京省'),('200','广东省'),('210','上海省'),('220','天津省'),('230','重庆省'),('240','辽宁省'),('250','江苏省'),('270','湖北省'),('280','四川省'),('290','陕西省'),('311','河北省'),('351','山西省'),('371','河南省'),('431','吉林省'),('451','黑龙江省'),('471','内蒙古省'),('531','山东省'),('551','安徽省'),('571','浙江省'),('591','福建省'),('731','湖南省'),('771','广西省'),('791','江西省'),('851','贵州省'),('871','云南省'),('891','西藏省'),('898','海南省'),('931','甘肃省'),('951','宁夏省'),('971','青海省'),('991','新疆省'),('-2','其他省')




CREATE TABLE `ham.ads_rpt_ev_ch_tr_di`(
  `tr_id` int COMMENT '码表id', 
  `busi_name` string, 
  `page_name` string, 
  `event_name` string, 
  `event` string, 
  `is_homepage` string, 
  `channel_id` string, 
  `channel_name` string, 
  `channel_type` string, 
  `page_id` string, 
  `mark2` string, 
  `mark3` string, 
  `mark4` string, 
  `mark5` string, 
  `pv` int COMMENT '事件点击次数', 
  `uv` int COMMENT 'ck_id去重统计')
COMMENT 'tableau 事件渠道汇总统计'
PARTITIONED BY ( 
  `dt` string)
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
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/ads_rpt_ev_ch_tr_di'
TBLPROPERTIES (
  'last_modified_by'='udbac', 
  'last_modified_time'='1595403326', 
  'serialization.null.format'='', 
  'transient_lastDdlTime'='1595403326')

  CREATE TABLE ham.dim_province_code_local on cluster cluster_gio_with_shard
(
    code String,
    province String

)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_province_code_local', '{replica}')
ORDER BY province
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.dim_province_code_all on cluster cluster_gio_with_shard
as ham.dim_province_code_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_province_code_local', rand());


















CREATE TABLE `dim_dcslog_event_busi`(
  `id` int COMMENT '唯一 id', 
  `funnel_name` string COMMENT '漏斗名称', 
  `funnel_no` int COMMENT '漏斗码', 
  `busi_name` string COMMENT '业务名称', 
  `page_name` string COMMENT '页面名称', 
  `event_name` string COMMENT '事件名称', 
  `event` string COMMENT '事件码', 
  `is_homepage` string COMMENT '是否首页', 
  `comment` string COMMENT '备注', 
  `url` string COMMENT '链接', 
  `data_source` string, 
  `mc_ev` string, 
  `type_name` string)
COMMENT 'dcslog业务event码表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/dim_dcslog_event_busi'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'last_modified_by'='udbac', 
  'last_modified_time'='1638791381', 
  'numFiles'='1', 
  'numRows'='-1', 
  'rawDataSize'='-1', 
  'totalSize'='2798398', 
  'transient_lastDdlTime'='1638791381')


  #分布式表——本地
CREATE TABLE ham.dim_dcslog_event_busi_local on cluster cluster_gio_with_shard
(
    id Nullable(String),
    funnel_name Nullable(String),
    funnel_no Nullable(String),
    busi_name Nullable(String),
    page_name Nullable(String),
    event_name Nullable(String),
    event  String,
    is_homepage Nullable(String),
    comment Nullable(String),
    url Nullable(String),
    data_source Nullable(String),
    mc_ev Nullable(String),
    type_name Nullable(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_dcslog_event_busi_local', '{replica}')
ORDER BY event
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.dim_dcslog_event_busi_all on cluster cluster_gio_with_shard
as ham.dim_dcslog_event_busi_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_dcslog_event_busi_local', rand());