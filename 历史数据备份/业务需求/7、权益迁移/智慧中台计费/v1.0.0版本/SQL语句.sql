建表语句
日提交
CREATE TABLE ods_zhzt_jf_di(
num int,
flow_code string,
ability_code string,
service_code string,
order_code string,
sub_order_code string,
charge_id string,
charge_rule_id string,
company string,
count_by_prvo string,
reserve_1 string,
reserve_2 string,
reserve_3 string,
day string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='#_#', 
  'serialization.null.format'='--') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  
建表语句
月提交
CREATE TABLE ods_zhzt_jf_dm(
num int,
ability_code string,
service_code string,
order_code string,
sub_order_code string,
charge_id string,
charge_rule_id string,
company string,
count_by_prvo string,
reserve_1 string,
reserve_2 string,
reserve_3 string,
month_time string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='#_#', 
  'serialization.null.format'='--') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  
  
查询日统计


set hive.default.fileformat = TextFile;
set hive.execution.engine = mr;
set hive.vectorized.execution.enabled  = false;

alter table ham_jituan.ods_zhzt_jf_di drop partition (dt='${DT}');
insert into table ham_jituan.ods_zhzt_jf_di partition (dt='${DT}')
select
row_number() over() as hanghao,
concat('${FT}',flow_code_sub) liushui,
ability_code,
service_code,
order_code,
sub_order_code,
charge_id,
charge_rule_id,
company,
jishu,
'',
'',
'',
'${FT}'
from
(
select
wt_prov,
count(*) jishu
from ham_jituan.dwd_client_event_di
where dt = '${DT}'
and wt_prov in (280,100,551,851,771,250)
group by wt_prov
) a join ham_jituan.ods_zhzt_jf_dim b
on a.wt_prov = b.prov;


查找月数据

set hive.default.fileformat = TextFile;
set hive.execution.engine = mr;
set hive.vectorized.execution.enabled  = false;

alter table ham_jituan.ods_zhzt_jf_dm drop partition (dt='${DT}');
insert into table ham_jituan.ods_zhzt_jf_dm partition (dt='${DT}')
select
row_number() over() as hanghao,
ability_code,
service_code,
order_code,
sub_order_code,
charge_id,
charge_rule_id,
company,
jishu,
'',
'',
'',
'${FT}'
from
(
select
wt_prov,
count(*) jishu
from ham_jituan.dwd_client_event_di
where
dt BETWEEN add_months(concat('${DT}','-27'),-1)  and concat('${DT}','-26')
and wt_prov in (280,100,551,851,771,250)
group by wt_prov
) a join ham_jituan.ods_zhzt_jf_dim b
on a.wt_prov = b.prov;