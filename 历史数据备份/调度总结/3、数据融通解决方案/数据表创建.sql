--测试数据库
CREATE TABLE test_dwd_51006_YS(
  `user` string,
  `user_key` string,
  `event_time` string,
  `domain` string, 
  `trmnl_style` string, 
  `true_data` string)
COMMENT '51006数据融通dwd测试'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/test_dwd_51006_YS'


CREATE TABLE test_dim_domain_ys(
  `domain` string, 
  `trmnl_style` string, 
  `true_data` string)
COMMENT '51006数据融通dim测试'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/test_dim_domain_ys'

insert into table ham.test_dwd_51006_YS
select 
user,
user_key,
event_time,
domain,
data_source_id,
''
from ham_jituan.ods_client_event_gio_all
where dt = '2023-08-27'
and hour = '10'
and data_source_id in ('a1f48d9ff4f42571','b508a809cbbddd0b')
and get_json_object(attributes, '$.WT_av') is null 
and event_type = 'custom_event'
limit 25;

--插入数据
insert into table test_dim_domain_ys (domain,trmnl_style,true_data) 
values ('m.sd.10086.cn','b508a809cbbddd0b','aa410d5cd21666f5')
--('hb.ac.10086.cn','b508a809cbbddd0b','aa410d5cd21666f5'),
('wap.gs.10086.cn','b508a809cbbddd0b','aa410d5cd21666f5'),
('com.greenpoint.android.mc10086.activity','b508a809cbbddd0b','a1f48d9ff4f42571')
;


insert into table test_dwd_51006_YS(user,user_key,event_time,domain,trmnl_style,true_data)
values
('19862544356','$basic_userId','2023-08-27 10:02:43.915','m.sd.10086.cn','b508a809cbbddd0b',''),
('18293131313','$basic_userId','2023-08-27 10:03:02.916','wap.gs.10086.cn','b508a809cbbddd0b','')
('18293131313','$basic_userId','2023-08-27 10:03:02.916','wap.gs.10086.cn','b508a809cbbddd0b','')
('18293131313','$basic_userId','2023-08-27 10:03:02.916','wap.gs.10086.cn','b508a809cbbddd0b','')
('18293131313','$basic_userId','2023-08-27 10:03:02.916','wap.gs.10086.cn','b508a809cbbddd0b','')
;

--查询维表中对应渠道的domain数据
select
domain,
trmnl_style,
true_data
from test_dim_domain_ys
where true_data in ('aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2')

select 
domain
from ham_jituan.ods_client_event_gio_all
where dt = '2023-08-27'
and hour = '10'
and data_source_id in ('a1f48d9ff4f42571','b508a809cbbddd0b')
and get_json_object(attributes, '$.WT_av') is null 
and event_type = 'custom_event'
group by domain
limit 25;

--中间表和维表关联映射中维表中包含的domain数据
select
tb1.user,
tb1.user_key,
tb1.event_time,
tb1.domain,
tb2.domain,
tb1.trmnl_style,
tb2.trmnl_style, 
tb2.true_data
from ham.test_dwd_51006_YS tb1
inner join (
select
domain,
trmnl_style,
true_data
from ham.test_dim_domain_ys
where true_data in ('aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2')) tb2 
on tb1.domain=tb2.domain;


select count(1) from ham.test_dwd_51006_YS;
select count(1) from ham.test_dim_domain_ys;
select * from ham.test_dwd_51006_YS;
select * from ham.test_dim_domain_ys;



















CREATE TABLE `ads_rpt_hachi_51006_dt_hour`(
  `daytime` string COMMENT 'from deserializer', 
  `ed` string COMMENT 'from deserializer', 
  `xy` string COMMENT 'from deserializer', 
  `ip_city` string COMMENT 'from deserializer', 
  `wt_input_mobile` string COMMENT 'from deserializer', 
  `wt_goods_specs` string COMMENT 'from deserializer', 
  `wt_goods_price` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `dt` string, 
  `hour` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='#_#') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/ads_rpt_hachi_51006_dt_hour'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'last_modified_by'='udbac', 
  'last_modified_time'='1689131799', 
  'numFiles'='23', 
  'numRows'='67598281', 
  'rawDataSize'='0', 
  'totalSize'='2697495630', 
  'transient_lastDdlTime'='1689131799')
101 rows selected (0.317 seconds)