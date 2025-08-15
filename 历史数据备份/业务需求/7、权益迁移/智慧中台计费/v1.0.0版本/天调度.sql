set hive.default.fileformat = TextFile;
set hive.execution.engine = mr;
set hive.vectorized.execution.enabled  = false;

alter table ham_jituan.ods_zhzt_jf_di drop partition (dt='${DT}');
insert into table ham_jituan.ods_zhzt_jf_di partition (dt='${DT}')
select
row_number() over() as hanghao,
concat('${FT}',flow_code_sub) liushui,
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
'${FT}'
from
(
select
wt_prov,
count(*) jishu
from ham_jituan.dwd_client_event_di
where dt = '${DT}'
and wt_prov in (280,100,551,851,771,250)
group by wt_prov
) a join ham_jituan.ods_zhzt_jf_dim b
on a.wt_prov = b.prov;
