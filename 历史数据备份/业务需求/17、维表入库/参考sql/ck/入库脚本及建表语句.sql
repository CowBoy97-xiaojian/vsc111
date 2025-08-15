
--100  ham.dim_order_details
#!/bin/bash
		clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="alter table ham.dim_order_detail_local on cluster cluster_gio_with_shard drop partition '$1'"
		
		clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="truncate table ham.dim_order_detail_all" < 0002_00100_20230326.txt


		
--101  ham.dim_order_activity
#!/bin/bash
		clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="alter table ham.dim_order_activity_local on cluster cluster_gio_with_shard drop partition '$1'"

		clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="insert into ham.dim_order_activity_all FORMAT CSV" < /home/udbac/af_input/$1/"0002_00101_"$1".txt"
		
		
--200  ham.dim_order_channel
#!/bin/bash
		clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="alter table ham.dim_order_channel_local on cluster cluster_gio_with_shard drop partition '$1'"
		
		clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --input_format_allow_errors_ratio 0.05 --format_csv_delimiter=$'|' --query="insert into ham.dim_order_channel_all FORMAT CSV" < /home/udbac/af_input/$1/"0002_00200_"$1".txt"
		
		
		
--201  ham.dim_order_page
#!/bin/bash
		clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="alter table ham.dim_order_page_local on cluster cluster_gio_with_shard drop partition '$1'"
		
		clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="insert into ham.dim_order_page_all FORMAT CSV" < /home/udbac/af_input/$1/"0002_00201_"$1".txt"
		
--202  ham.dim_order_product
#!/bin/bash
		clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="alter table ham.dim_order_product_local on cluster cluster_gio_with_shard drop partition '$1'"
		
		clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="insert into ham.dim_order_product_all FORMAT CSV" < /home/udbac/af_input/$1/"0002_00202_"$1".txt"
			
--权益入中间表

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$',' --query="truncate table ham.dim_qycs_component_local1 on cluster cluster_gio_with_shard "

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$',' --query="insert into ham.dim_qycs_component_all FORMAT CSV" < /home/udbac/af_input/ck_input/ck_jy_qycs_component/QYCS_EventTrackingMapping.csv






        
--100建表

  `create_date` string COMMENT '订单日期', 
  `province_name` string COMMENT '省份', 
  `page_id` string COMMENT '页面编码', 
  `touch_code` string COMMENT '四级渠道ID', 
  `touch_name` string COMMENT '四级渠道名称', 
  `seller_id` string COMMENT '销售员工号', 
  `goods_id` string COMMENT '能开商品编码', 
  `goods_name` string COMMENT '能开商品名称', 
  `order_type` string COMMENT '产品类型', 
  `sales` int COMMENT '产品销量')

  
CREATE TABLE ham.dim_order_detail_local on cluster cluster_gio_with_shard
(
    create_date String,
    province_name String,
    page_id String,
    touch_code String,
    touch_name String,
    seller_id String,
    goods_id String,
    goods_name String,
    order_type String,
    sales int
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_detail_local', '{replica}')
PARTITION BY create_date
ORDER BY create_date
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.dim_order_detail_all on cluster cluster_gio_with_shard
as ham.dim_order_detail_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_detail_local', rand());




--101  ham.dim_order_activity
CREATE TABLE `dim_order_activity`(
  `create_date` string COMMENT '订单日期', 
  `province_name` string COMMENT '省份', 
  `channel_id` string COMMENT '页面编码', 
  `channel_name` string COMMENT '四级渠道ID', 
  `seller_id` string COMMENT '销售员工号', 
  `activity_id` string COMMENT '活动id', 
  `activity_name` string COMMENT '活动名称', 
  `sku_code` string COMMENT '奖品编码', 
  `sku_name` int COMMENT '奖品	名称', 
  `sales` int COMMENT '奖品销量')
PARTITIONED BY ( 
  `dt` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/dim_order_activity'
TBLPROPERTIES (
  'transient_lastDdlTime'='1603703148')
  
CREATE TABLE ham.dim_order_activity_local on cluster cluster_gio_with_shard
(
    create_date String,
    province_name String,
    channel_id String,
    channel_name String,
    seller_id String,
    activity_name String,
    sku_code String,
    sku_name String,
    sales int
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_activity_local', '{replica}')
PARTITION BY create_date
ORDER BY create_date
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.dim_order_activity_all on cluster cluster_gio_with_shard
as ham.dim_order_activity_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_activity_local', rand());


--200  ham.dim_order_channel
CREATE EXTERNAL TABLE `dim_order_channel`(
  `create_date` string COMMENT '订单日期', 
  `channel_id_l4` string COMMENT '四级渠道编码', 
  `channel_id_l3` string COMMENT '三级渠道编码', 
  `channel_id_l2` string COMMENT '二级渠道编码', 
  `channel_id_l1` string COMMENT '一级渠道编码', 
  `channel_name_l4` string COMMENT '四级渠道名称', 
  `channel_name_l3` string COMMENT '三级渠道名称', 
  `channel_name_l2` string COMMENT '二级渠道名称', 
  `channel_name_l1` string COMMENT '一级渠道名称', 
  `channel_type` string COMMENT '渠道类型')
PARTITIONED BY ( 
  `dt` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/dim_order_channel'
TBLPROPERTIES (
  'last_modified_by'='udbac', 
  'last_modified_time'='1605150271', 
  'transient_lastDdlTime'='1605150271')
  
CREATE TABLE ham.dim_order_channel_local on cluster cluster_gio_with_shard
(
    create_date String,
    channel_id_l4 String,
    channel_id_l3 String,
    channel_id_l2 String,
    channel_id_l1 String,
    channel_name_l4 String,
    channel_name_l3 String,
    channel_name_l2 String,
    channel_name_l1 String,
    channel_type String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_channel_local', '{replica}')
PARTITION BY create_date
ORDER BY create_date
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.dim_order_channel_all on cluster cluster_gio_with_shard
as ham.dim_order_channel_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_channel_local', rand());


--201  ham.dim_order_page
CREATE TABLE `dim_order_page`(
  `statis_date` string COMMENT '更新日期', 
  `activity_type` string COMMENT '活动类型', 
  `page_id` string COMMENT '页面编码', 
  `page_type` string COMMENT '页面类型', 
  `page_name` string COMMENT 'pageid对应的页面名称', 
  `app_name` string COMMENT '应用名称', 
  `channel_id` string COMMENT '四级渠道编码', 
  `url` string COMMENT '链接地址', 
  `url_status` string COMMENT '链接状态', 
  `start_time` string COMMENT '开始时间', 
  `end_time` string COMMENT '结束时间')
PARTITIONED BY ( 
  `dt` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/dim_order_page'
TBLPROPERTIES (
  'transient_lastDdlTime'='1617762737')
  
CREATE TABLE ham.dim_order_page_tmp_local on cluster cluster_gio_with_shard
(
statis_date String,
activity_type String,
page_id String,
page_type String,
page_name String,
app_name String,
channel_id String,
url String,
url_status String,
start_time String,
end_time String
)   
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_page_tmp_local', '{replica}')
ORDER BY page_id
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.dim_order_page_tmp_all on cluster cluster_gio_with_shard
as ham.dim_order_page_tmp_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_page_tmp_local', rand());


CREATE TABLE ham.dim_order_page_local on cluster cluster_gio_with_shard
(
statis_date String,
activity_type String,
page_id String,
page_type String,
page_name String,
app_name String,
channel_id String,
url String,
url_status String,
start_time String,
end_time String,
current_time String
)   
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_page_local', '{replica}')
PARTITION BY current_time
ORDER BY current_time
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';
  
 CREATE TABLE ham.dim_order_page_all on cluster cluster_gio_with_shard
as ham.dim_order_page_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_page_local', rand());

drop table ham.dim_order_page_local on cluster cluster_gio_with_shard;
drop table ham.dim_order_page_all on cluster cluster_gio_with_shard;

ALTER TABLE  ham.dim_order_page_local on cluster cluster_gio_with_shard RENAME COLUMN d_time to end_time;
ALTER TABLE  ham.dim_order_page_local on cluster cluster_gio_with_shard RENAME COLUMN t_time to current_time;
  
  

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="truncate table ham.dim_order_page_tmp"
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="insert into ham.dim_order_page_tmp FORMAT CSV" < /home/udbac/af_input/$1/"0002_00202_"$1".txt"
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="alter table ham.dim_order_page_local on cluster cluster_gio_with_shard drop partition '$1'"
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="insert into ham.dim_order_page_all(statis_date,activity_type,page_id,page_type,page_name,app_name,channel_id,url,url_status,start_time,end_time,current_time) SELECT statis_date,activity_type,page_id,page_type,page_name,app_name,channel_id,url,url_status,start_time,end_time,'$1' FROM ham.dim_order_page_tmp;"	
		
		
statis_date,activity_type,page_id,page_type,page_name,app_name,channel_id,url,url_status,start_time,end_time,current_time
		
		
insert into ham.dim_order_page_all(statis_date,activity_type,page_id,product_id,goods_id,goods_name,activity_id,current_time) SELECT statis_date,activity_type,page_id,product_id,goods_id,goods_name,activity_id,'20230326' FROM ham.dim_order_page_tmp;
 
  
  
  
  
  
  
  
  
  
  

--202  ham.dim_order_product

CREATE TABLE `dim_order_product`(
  `statis_date` string COMMENT '更新日期', 
  `activity_type` string COMMENT '活动类型', 
  `page_id` string COMMENT '页面编码', 
  `product_id` string COMMENT '商品编码', 
  `goods_id` string COMMENT '能开编码', 
  `goods_name` string COMMENT '产品名称', 
  `activity_id` string COMMENT '关联活动ID')
PARTITIONED BY ( 
  `dt` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/dim_order_product'
TBLPROPERTIES (
  'transient_lastDdlTime'='1617762958')
  
CREATE TABLE ham.dim_order_product_local on cluster cluster_gio_with_shard
(
statis_date String,
activity_type String,
page_id String,
product_id String,
goods_id String,
goods_name String,
activity_id String,
current_time String
)   
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_product_local1', '{replica}')
PARTITION BY current_time
ORDER BY current_time
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk'; 
  
CREATE TABLE ham.dim_order_product_all on cluster cluster_gio_with_shard
as ham.dim_order_product_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_product_local', rand());


CREATE TABLE ham.dim_order_product_tmp_local on cluster cluster_gio_with_shard
(
    statis_date String,
    activity_type String,
    page_id String,
    product_id String,
    goods_id String,
    goods_name String,
    activity_id String
)   
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_product_tmp_local', '{replica}')
ORDER BY activity_id
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk'; 

CREATE TABLE ham.dim_order_product_tmp_all on cluster cluster_gio_with_shard
as ham.dim_order_product_tmp_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_product_tmp_local', rand());

drop table ham.dim_order_product_all on cluster cluster_gio_with_shard;



--202数据入临时表--202  ham.dim_order_product
#!/bin/bash
		

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="truncate table ham.dim_order_product_tmp"
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="insert into ham.dim_order_product_tmp FORMAT CSV" < /home/udbac/af_input/$1/"0002_00202_"$1".txt"
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="alter table ham.dim_order_product_local on cluster cluster_gio_with_shard drop partition '$1'"
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="insert into ham.dim_order_product_all(statis_date,activity_type,page_id,product_id,goods_id,goods_name,activity_id,current_time) SELECT statis_date,activity_type,page_id,product_id,goods_id,goods_name,activity_id,'$1' FROM ham.dim_order_product_tmp;"	
		
		
		
		
insert into ham.dim_order_product_all(statis_date,activity_type,page_id,product_id,goods_id,goods_name,activity_id,current_time) SELECT statis_date,activity_type,page_id,product_id,goods_id,goods_name,activity_id,'20230326' FROM ham.dim_order_product_tmp;
 
		























  


--QYCS_EventTrackingMapping dim_qycs_component
CREATE TABLE `dim_qycs_component`(
  `component_id` string COMMENT '组件母版id', 
  `component_name` string COMMENT '组件母版名称', 
  `point_position` string COMMENT '组件位置id', 
  `point_position_name` string COMMENT '组件位置名称')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/dim_qycs_component'
TBLPROPERTIES (
  'transient_lastDdlTime'='1635234437')
  
 CREATE TABLE ham.dim_qycs_component_local1 on cluster cluster_gio_with_shard
(
    component_id String,
    component_name String,
    point_position String,
    point_position_name String 
)   
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_qycs_component_local1', '{replica}')
ORDER BY component_id
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk'; 


CREATE TABLE ham.dim_qycs_component_all on cluster cluster_gio_with_shard
as ham.dim_qycs_component_local1
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_qycs_component_local1', rand());


CREATE TABLE ham.dim_qycs_component_tmp
(
    component_id String,
    component_name String,
    point_position String,
    point_position_name String 
)   
ENGINE = MergeTree
ORDER BY component_id
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$',' --query="inset into "