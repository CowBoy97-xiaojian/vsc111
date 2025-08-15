#查询日统计

#!/bin/bash

DTF=$1
DT=$(date -d $DTF +"%Y-%m-%d")

#清空当前分区下的数据
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="alter table ham.ads_zhzt_jf_di_local on cluster cluster_gio_with_shard drop partition '${DTF}'"

insert into table ham.ads_zhzt_jf_di_all on cluster cluster_gio_with_shard
select
rowNumberInAllBlocks()+1 as hanghao,
concat('${DTF}',flow_code_sub) liushui,
ability_code,
service_code,
order_code,
sub_order_code,
charge_id,
charge_rule_id,
company,
jishu,
'',
'',
'',
'${DTF}'
from
(
select
wt_prov,
count(*) jishu
from ham_jituan.dwd_client_event_di
where dt = '${DTF}'
and wt_prov in (280,100,551,851,771,250)
group by wt_prov
) a join ham.dim_ads_zhzt_jf_all b
on a.wt_prov = b.prov;


#查找月数据

#清空当前分区下的数据
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="alter table ham.ads_zhzt_jf_dm_local on cluster cluster_gio_with_shard drop partition '${DTF}'"


insert into table ham.ads_zhzt_jf_dm_all on cluster cluster_gio_with_shard
select
rowNumberInAllBlocks()+1 as hanghao,
ability_code,
service_code,
order_code,
sub_order_code,
charge_id,
charge_rule_id,
company,
jishu,
'',
'',
'',
'${DTF}'
from
(
select
wt_prov,
count(*) jishu
from ham_jituan.dwd_client_event_di
where
dt BETWEEN add_months(concat('${DT}','-27'),-1)  and concat('${DT}','-26')
and wt_prov in (280,100,551,851,771,250)
group by wt_prov
) a join ham.dim_ads_zhzt_jf_all b
on a.wt_prov = b.prov;