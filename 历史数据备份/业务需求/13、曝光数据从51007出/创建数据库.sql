create database ham on cluster cluster_gio_with_shard;
create database webtrends on cluster cluster_gio_with_shard;

--创建51007中间表，CREATE TABLE webtrends.event_hi_client_local on cluster cluster_gio_with_shard