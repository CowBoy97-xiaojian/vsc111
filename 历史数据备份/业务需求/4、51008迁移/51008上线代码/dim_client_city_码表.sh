
#分布式表——本地
CREATE TABLE ham.dim_client_city_local on cluster cluster_gio_with_shard
(
    city      String, 
    city_name String, 
    prov      String,
    name      String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_client_city_local', '{replica}')
ORDER BY city
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.dim_client_city_all on cluster cluster_gio_with_shard
as ham.dim_client_city_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_client_city_local', rand());

insert into table ham.dim_client_city_local select * from ham.dim_client_city;