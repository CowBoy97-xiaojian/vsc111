CREATE TABLE ham.rule_engine_local on cluster cluster_gio_with_shard
(
marketing_id Nullable(String),
area_location Nullable(String),
area_name Nullable(String),
dt String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.rule_engine_local', '{replica}')
PARTITION BY dt
ORDER BY dt
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.rule_engine_all on cluster cluster_gio_with_shard
as ham.rule_engine_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'rule_engine_local', rand());


INSERT INTO ham.dim_client_h5_all (pindao,quyu,event)
values
('573_90044','流量专区-asdf','业务楼层区域','2023-06-26'),
('573_90044','套餐专区-dasdf','业务楼层区域','2023-06-26'),
('573_90044','终端专区-dsds','业务楼层区域','2023-06-26');