select 
mc_ev
,event
,et
,attributes
from webtrends.event_hi_dcslog_51006_all
where dt='2023-05-31'
and dcsid='aba9de4ce446b2d2'
and mc_ev = '210315_QYCS_fqdsyt'
FORMAT CSVWithNames;

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="truncate table ham.dim_client_ld_zdy_d_tmp"
