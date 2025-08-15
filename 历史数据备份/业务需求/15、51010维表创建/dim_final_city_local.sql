#分布式表——本地
CREATE TABLE ham.dim_final_city_local on cluster cluster_gio_with_shard
(
    prov_name            String, 
    city_name            String, 
    final_city_name      String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_final_city_local', '{replica}')
ORDER BY city_name
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.dim_final_city_all on cluster cluster_gio_with_shard
as ham.dim_final_city_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_final_city_local', rand());


clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$',' --query="insert into table ham.dim_final_city_all " < dim_final_city.csv

clickhouse-client -h 10.104.24.235 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$',' --query="insert into table ham.dim_final_city_all FORMAT CSV" < dim_final_city.csv