
ham.dim_qycs_channel


col_name,data_type,comment
channelid,string,老渠道号
touch_code,string,新渠道号


CREATE TABLE ham.dim_qycs_channel_local on cluster cluster_gio_with_shard
(
    channelid String,
    touch_code String
)   
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_qycs_channel_local', '{replica}')
ORDER BY channelid
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


CREATE TABLE ham.dim_qycs_channel_all on cluster cluster_gio_with_shard
as ham.dim_qycs_channel_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_qycs_channel_local', rand());
