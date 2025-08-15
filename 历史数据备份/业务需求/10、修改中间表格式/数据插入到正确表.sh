

clickhouse-client -h 10.253.248.73 -m --database="webtrends" --receive_timeout=3600 --query="insert into table webtrends.event_hi_client_t_all select * from webtrends.event_hi_client_all"


clickhouse-client -h 10.253.248.73 -m --database="webtrends" --receive_timeout=3600 --query="insert into table webtrends.event_hi_dcslog_t_all select * from webtrends.event_hi_dcslog_all1 where dt = '2023-06-06' and hour = '01'"



insert into table webtrends.event_hi_dcslog_all select * from webtrends.event_hi_dcslog_all1 where dt = '2023-06-06' and hour = '00'


alter table webtrends.event_hi_dcslog_local on cluster cluster_gio_with_shard delete where dt = '2023-06-06' and hour = '11';
