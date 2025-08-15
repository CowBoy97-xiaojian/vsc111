


#rowNumberInAllBlocks()+1   ---数据插入行数

#1、权益月数据表创建
行号    能力编码    服务编码    订单编码    子订单编码  计费模型id  计费规则id  计量单位编码    计量总量    预留字段123 月账期  
1,A202900002,S10291200038,AOR202212120111,AOR20221212011101,85741,,0203,658710805,,,,202304,2023-04

CREATE TABLE ham.ads_zhzt_jf_dm_local on cluster cluster_gio_with_shard (
    num Nullable(String) comment '行号',
    ability_code Nullable(String) comment '能力编码',
    service_code Nullable(String) comment '服务编码',
    order_code Nullable(String) comment '订单编码',
    sub_order_code Nullable(String) comment '子订单编码',
    charge_id Nullable(String) comment '计费模型id',
    charge_rule_id Nullable(String) comment '计费规则id',
    company Nullable(String) comment '计量单位编码',
    count_by_prvo Nullable(String) comment '计量总量',
    reserve_1 Nullable(String) comment '预留字段1',
    reserve_2 Nullable(String) comment '预留字段2',
    reserve_3 Nullable(String) comment '预留字段3',
    dt String comment '月账期'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.ads_zhzt_jf_dm_local', '{replica}')
PARTITION BY dt
ORDER BY dt
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

#视图
CREATE TABLE ham.ads_zhzt_jf_dm_all on cluster cluster_gio_with_shard
as ham.ads_zhzt_jf_dm_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'ads_zhzt_jf_dm_local', rand());



#2、权益日数据表创建
行号    唯一流水号  能力编码    服务编码    订单编码    子订单编码  计费模型id  计费规则id  计量单位编码    计量数值    预留字段123 日账期
1,20230515A202900002s1p250,A202900002,S10291200038,AOR202212120111,AOR20221212011101,85741,,0203,18465561,,,,20230515,2023-05-15

CREATE TABLE ham.ads_zhzt_jf_di_local on cluster cluster_gio_with_shard (
    num Nullable(String) comment '行号',
    flow_code String comment '唯一流水号',
    ability_code Nullable(String) comment '能力编码',
    service_code Nullable(String) comment '服务编码',
    order_code Nullable(String) comment '订单编码',
    sub_order_code Nullable(String) comment '子订单编码',
    charge_id Nullable(String) comment '计费模型id',
    charge_rule_id Nullable(String) comment '计费规则id',
    company Nullable(String) comment '计量单位编码',
    count_by_prvo Nullable(String) comment '计量数值',
    reserve_1 Nullable(String) comment '预留字段1',
    reserve_2 Nullable(String) comment '预留字段2',
    reserve_3 Nullable(String) comment '预留字段3',
    dt String comment '日账期'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.ads_zhzt_jf_di_local', '{replica}')
PARTITION BY dt
ORDER BY (dt,flow_code)
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

#视图
CREATE TABLE ham.ads_zhzt_jf_di_all on cluster cluster_gio_with_shard
as ham.ads_zhzt_jf_di_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'ads_zhzt_jf_di_local', rand());


#3、中间表数据创建
省份编码    流水号一部分    能力编码    服务编码    订单编码    子订单编码  计费模型id  计费规则id  计量单位编码
280,A202900002s1p280,A202900002,S10291200038,AOR202211170088,AOR20221117008801,85741,,0203
CREATE TABLE ham.dim_ads_zhzt_jf_local on cluster cluster_gio_with_shard (
prov String comment '省份编码',
flow_code_sub Nullable(String) comment '流水号一部分',
ability_code Nullable(String) comment '能力编码',
service_code Nullable(String) comment '服务编码',
order_code Nullable(String) comment '订单编码',
sub_order_code Nullable(String) comment '子订单编码',
charge_id Nullable(String) comment '计费模型id',
charge_rule_id Nullable(String) comment '计费规则id',
company Nullable(String) comment '计量单位编码'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_ads_zhzt_jf_local', '{replica}')
ORDER BY prov
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

#视图
CREATE TABLE ham.dim_ads_zhzt_jf_all on cluster cluster_gio_with_shard
as ham.dim_ads_zhzt_jf_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_ads_zhzt_jf_local', rand());
insert into table ham.dim_ads_zhzt_jf_all values
('280','A202900002s1p280','A202900002','S10291200038','AOR202211170088','AOR20221117008801','85741','','0203'),
('280','A202900002s2p280','A202900002','S10291200039','AOR202211170088','AOR20221117008801','85751','','0203'),
('551','A202900002s2p551','A202900002','S10291200039','AOR202211290025','AOR20221129002501','85751','','0203'),
('851','A202900002s1p851','A202900002','S10291200038','AOR202212010001','AOR20221201000101','85741','','0203'),
('851','A202900002s2p851','A202900002','S10291200039','AOR202212010001','AOR20221201000101','85751','','0203'),
('250','A202900002s1p250','A202900002','S10291200038','AOR202212120111','AOR20221212011101','85741','','0203'),
('280','A202900002s3p280','A202900002','S10291200040','AOR202211170088','AOR20221117008801','85761','','0203'),
('100','A202900002s1p100','A202900002','S10291200038','AOR202211290017','AOR20221129001701','85741','','0203'),
('100','A202900002s2p100','A202900002','S10291200039','AOR202211290017','AOR20221129001701','85751','','0203'),
('100','A202900002s3p100','A202900002','S10291200040','AOR202211290017','AOR20221129001701','85761','','0203'),
('771','A202900002s1p771','A202900002','S10291200038','AOR202211300100','AOR20221130010001','85741','','0203'),
('771','A202900002s2p771','A202900002','S10291200039','AOR202211300100','AOR20221130010001','85751','','0203'),
('771','A202900002s3p771','A202900002','S10291200040','AOR202211300100','AOR20221130010001','85761','','0203'),
('551','A202900002s1p551','A202900002','S10291200038','AOR202211290025','AOR20221129002501','85741','','0203');

prov,string,
flow_code_sub,string,
ability_code,string,
service_code,string,
order_code,string,
sub_order_code,string,
charge_id,string,
charge_rule_id,string,
company,string,





col_name,data_type,comment
prov,string,
flow_code_sub,string,
ability_code,string,
service_code,string,
order_code,string,
sub_order_code,string,
charge_id,string,
charge_rule_id,string,
company,string,


ods_zhzt_jf_dim.prov,ods_zhzt_jf_dim.flow_code_sub,ods_zhzt_jf_dim.ability_code,ods_zhzt_jf_dim.service_code,ods_zhzt_jf_dim.order_code,ods_zhzt_jf_dim.sub_order_code,ods_zhzt_jf_dim.charge_id,ods_zhzt_jf_dim.charge_rule_id,ods_zhzt_jf_dim.company
280,A202900002s1p280,A202900002,S10291200038,AOR202211170088,AOR20221117008801,85741,,0203
280,A202900002s2p280,A202900002,S10291200039,AOR202211170088,AOR20221117008801,85751,,0203
551,A202900002s2p551,A202900002,S10291200039,AOR202211290025,AOR20221129002501,85751,,0203
851,A202900002s1p851,A202900002,S10291200038,AOR202212010001,AOR20221201000101,85741,,0203
851,A202900002s2p851,A202900002,S10291200039,AOR202212010001,AOR20221201000101,85751,,0203
250,A202900002s1p250,A202900002,S10291200038,AOR202212120111,AOR20221212011101,85741,,0203
280,A202900002s3p280,A202900002,S10291200040,AOR202211170088,AOR20221117008801,85761,,0203
100,A202900002s1p100,A202900002,S10291200038,AOR202211290017,AOR20221129001701,85741,,0203
100,A202900002s2p100,A202900002,S10291200039,AOR202211290017,AOR20221129001701,85751,,0203
100,A202900002s3p100,A202900002,S10291200040,AOR202211290017,AOR20221129001701,85761,,0203
771,A202900002s1p771,A202900002,S10291200038,AOR202211300100,AOR20221130010001,85741,,0203
771,A202900002s2p771,A202900002,S10291200039,AOR202211300100,AOR20221130010001,85751,,0203
771,A202900002s3p771,A202900002,S10291200040,AOR202211300100,AOR20221130010001,85761,,0203
551,A202900002s1p551,A202900002,S10291200038,AOR202211290025,AOR20221129002501,85741,,0203



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