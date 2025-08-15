  #分布式表——本地
CREATE TABLE ham.dim_rpt_hachi_domain_local on cluster cluster_gio_with_shard
(
    interface String,
    domain String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_rpt_hachi_domain_local', '{replica}')
ORDER BY domain
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.dim_rpt_hachi_domain_all on cluster cluster_gio_with_shard
as ham.dim_rpt_hachi_domain_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_rpt_hachi_domain_local', rand());