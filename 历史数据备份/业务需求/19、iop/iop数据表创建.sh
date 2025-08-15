CREATE TABLE ham.rule_engine_local on cluster cluster_gio_with_shard
(
marketing_id Nullable(String),
area_location Nullable(String),
area_name String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.rule_engine_local', '{replica}')
ORDER BY area_name
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


CREATE TABLE ham.rule_engine_all on cluster cluster_gio_with_shard
as ham.rule_engine_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'rule_engine_local', rand());



select marketing_id,area_location from ham.dim_client_work_order_d_all where area_name='业务楼层区域' and dt 

select count(1) from ham.dim_client_work_order_d_all where area_name='业务楼层区域' and dt = '2023-06-24'



select area_location from ham.rule_engine_all 
where 
(position(area_location,'流量专区')>0 or position(area_location,'套餐专区')>0 or position(area_location,'终端专区')>0 or position(area_location,'权益专区')>0 ) limit 20;

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="insert into ham.rule_engine_all select marketing_id,area_location,area_name from ham.dim_client_work_order_d_all where area_name='业务楼层区域' and (position(area_location,'流量专区')>0 or position(area_location,'套餐专区')>0 or position(area_location,'终端专区')>0 or position(area_location,'权益专区')>0 ) and  dt ='${DT}'"

insert into ham.rule_engine_all select marketing_id,area_location,area_name from ham.dim_client_work_order_d_all where area_name='业务楼层区域' and (position(area_location,'流量专区')>0 or position(area_location,'套餐专区')>0 or position(area_location,'终端专区')>0 or position(area_location,'权益专区')>0 ) and  dt ='2023-07-04';