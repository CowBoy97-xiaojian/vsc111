set hive.default.fileformat = Orc;
set hive.execution.engine = mr;

insert into table ham.dwd_monitor_dh partition(type = '51006')
select dt
,hour
,count(1)
,'hour' as cycle
,unix_timestamp() as timestamp
from ham.ads_rpt_hachi_51006_dt_hour
where dt = '${DT}'
and hour = '${HH}'
group by dt,hour;

alter table ham.dwd_monitor_dh partition (type='client') concatenate;
alter table ham.dwd_monitor_dh partition (type='51006') concatenate;
[root@api-core-1 h5]# cat etl_a
etl_abs_tbau_pagetime_ch_di.hql
etl_abs_tbau_pagetime_di.hql
etl_ads_carnot_ch_pg_ev_di.hql
etl_ads_carnot_ch_pg_ev_di.hql_bak
etl_ads_carnot_ch_pg_prov_di.hql
etl_ads_carnot_ev_di.hql
etl_ads_carnot_fsuvzh_detail_di.hql
etl_ads_carnot_fsuvzh_di.hql
etl_ads_carnot_ldym_di.hql
etl_ads_carnot_order_ch_pg_di.hql
etl_ads_carnot_qqtly_wi.hql
etl_ads_carnot_referer_info_di.hql
etl_ads_ch_pa_ev_pr_fsuv_di.hql
etl_ads_rpt_ch_tr_di.hql
etl_ads_rpt_ch_tr_hi.hql
etl_ads_rpt_ev_ch_tr_di.hql
etl_ads_rpt_ev_ch_tr_hi.hql
etl_ads_rpt_ev_ch_tr_pr_di.hql
etl_ads_rpt_hachi_40001.hql
etl_ads_rpt_hachi_51006_dt_hour.hql
etl_ads_rpt_hachi_51006_dt_hour.hql_bak
etl_ads_rpt_hachi_51006_dt_hour.hql_bak20230720
etl_ads_rpt_hachi_51006_dt_test.hql
etl_ads_rpt_hachi_51006_gio_hourly.hql
etl_ads_rpt_hachi_51006_gio_hourly.hql.bak
etl_ads_rpt_hachi_51006_hourly.hql
etl_ads_rpt_hachi_51006_hourly.hql_bak20230718
etl_ads_rpt_hachi_51006.hql
etl_ads_rpt_hachi_51006.hql.bak
etl_ads_rpt_hachi_51010_hourly.hql
etl_ads_tbau_ch_pg_di.hql
etl_ads_tbau_ch_pg_hi.hql
etl_ads_tbau_ch_pr_di.hql
etl_ads_tbau_ev_ch_area_di.hql
etl_ads_tbau_ev_ch_area_hi.hql
etl_ads_tbau_ev_ch_area_hi.hql_bak
etl_ads_tbau_ev_ch_area_lj_di.hql
etl_ads_tbau_ev_ch_area_lj_hi.hql
etl_ads_tbau_ev_ch_pr_di.hql
etl_ads_tbau_ev_di.hql
etl_ads_tbau_ev_hi.hql
etl_ads_tbau_ldym_di.hql
etl_ads_tbau_oao_hi.hql
etl_ads_tbau_order_ch_pg_di.hql
etl_ads_tbau_qqtly_week.hql
etl_ads_tbau_referer_info_di.hql
etl_ads_tbau_uv_di.hql
etl_ads_tbau_zfbfffff_di.hql
[root@api-core-1 h5]# cat etl_a
etl_abs_tbau_pagetime_ch_di.hql
etl_abs_tbau_pagetime_di.hql
etl_ads_carnot_ch_pg_ev_di.hql
etl_ads_carnot_ch_pg_ev_di.hql_bak
etl_ads_carnot_ch_pg_prov_di.hql
etl_ads_carnot_ev_di.hql
etl_ads_carnot_fsuvzh_detail_di.hql
etl_ads_carnot_fsuvzh_di.hql
etl_ads_carnot_ldym_di.hql
etl_ads_carnot_order_ch_pg_di.hql
etl_ads_carnot_qqtly_wi.hql
etl_ads_carnot_referer_info_di.hql
etl_ads_ch_pa_ev_pr_fsuv_di.hql
etl_ads_rpt_ch_tr_di.hql
etl_ads_rpt_ch_tr_hi.hql
etl_ads_rpt_ev_ch_tr_di.hql
etl_ads_rpt_ev_ch_tr_hi.hql
etl_ads_rpt_ev_ch_tr_pr_di.hql
etl_ads_rpt_hachi_40001.hql
etl_ads_rpt_hachi_51006_dt_hour.hql
etl_ads_rpt_hachi_51006_dt_hour.hql_bak
etl_ads_rpt_hachi_51006_dt_hour.hql_bak20230720
etl_ads_rpt_hachi_51006_dt_test.hql
etl_ads_rpt_hachi_51006_gio_hourly.hql
etl_ads_rpt_hachi_51006_gio_hourly.hql.bak
etl_ads_rpt_hachi_51006_hourly.hql
etl_ads_rpt_hachi_51006_hourly.hql_bak20230718
etl_ads_rpt_hachi_51006.hql
etl_ads_rpt_hachi_51006.hql.bak
etl_ads_rpt_hachi_51010_hourly.hql
etl_ads_tbau_ch_pg_di.hql
etl_ads_tbau_ch_pg_hi.hql
etl_ads_tbau_ch_pr_di.hql
etl_ads_tbau_ev_ch_area_di.hql
etl_ads_tbau_ev_ch_area_hi.hql
etl_ads_tbau_ev_ch_area_hi.hql_bak
etl_ads_tbau_ev_ch_area_lj_di.hql
etl_ads_tbau_ev_ch_area_lj_hi.hql
etl_ads_tbau_ev_ch_pr_di.hql
etl_ads_tbau_ev_di.hql
etl_ads_tbau_ev_hi.hql
etl_ads_tbau_ldym_di.hql
etl_ads_tbau_oao_hi.hql
etl_ads_tbau_order_ch_pg_di.hql
etl_ads_tbau_qqtly_week.hql
etl_ads_tbau_referer_info_di.hql
etl_ads_tbau_uv_di.hql
etl_ads_tbau_zfbfffff_di.hql
[root@api-core-1 h5]# cat etl_ads_rpt_hachi_51006_dt_hour.hql
set hive.default.fileformat = Orc;
set hive.execution.engine = mr;

insert overwrite table ads_rpt_hachi_51006_dt_hour partition(dt = '${DT}', hour = '${HH}')
select daytime,
  ip,
  nullif(substr(regexp_extract(decode_url(query['WT.es']), 'channelId=([0-9a-zA-z]+)', 1),0,12),''),
  substr(nullif(regexp_extract(decode_url(query['WT.es']), 'pageId=([0-9a-zA-z]+)', 1),''), 0, 19),
  regexp_extract(decode_url(query['WT.es']), 'sellerId=([0-9-a-zA-z]+)',1),
  query['WT.dcsdat'] AS dcsdat,
  nullif(split(query['WT.channelid'],'%')[0], '') AS channelid,
  query['WT.co_f'] AS co_f,
  decode_url(query['WT.event']) AS event,
  regexp_replace(decode_url(query['WT.es']), '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') AS es,
  regexp_replace(query['WT.mc_ev'],'\\|','_') AS mc_ev,
  regexp_replace(query['WT.sellerid'],'\\|','_') AS sellerid,
  substr(nullif(regexp_replace(query['WT.pageid'],'\\|','_'), ''), 0, 19) AS pageid,
  regexp_replace(query['WT.last_pageid'],'\\|','_') AS last_pageid,
  regexp_replace(query['WT.last_url'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') AS last_url,
  decode_url(regexp_replace(query['WT.last_pagename'],'\\|','_')) AS last_pagename,
  regexp_replace(query['WT.current_url'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','_') AS current_url,
  decode_url(regexp_replace(query['WT.current_pagename'],'\\|','_')) AS current_pagename,
  regexp_replace(query['WT.module_no'],'\\|','_') AS module_no,
  decode_url(regexp_replace(query['WT.module_name'],'\\|','_')) AS module_name,
  regexp_replace(query['WT.point_position'],'\\|','_') AS point_position,
  decode_url(regexp_replace(query['WT.member'],'\\|','_')) AS member,
  regexp_replace(query['WT.goods_no'],'\\|','_') AS goods_no,
  regexp_replace(query['WT.login_status'],'\\|','_') AS login_status,
  regexp_replace(query['WT.next_pageid'],'\\|','_') AS next_pageid,
  regexp_replace(decode_url(regexp_replace(query['WT.next_url'],'\\|','_')), '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') AS next_url,
  decode_url(regexp_replace(query['WT.next_pagename'],'\\|','_')) AS next_pagename,
  regexp_replace(query['WT.component_id'],'\\|','_') AS component_id,
  regexp_replace(user_agent,'\\|','_'),
  regexp_replace(referer, '\\s+', ''),
  coalesce(mobile, first_value(mobile, TRUE)
                   OVER (PARTITION BY ck_id
                         ORDER BY daytime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS mobile,
  ss_id,
  regexp_replace(decode_url(decode_input(query['WT.input_sfz'])), '\\s+', ''),
  query['WT.environment'] AS environment,
  code,
--  case when (query['WT.environment']='production' and regexp_replace(query['WT.mc_ev'],'\\|','_')='210315_QYCS') then tb2.short_name
--  else code end as ip_prov,
  regexp_replace(query['WT.plat'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','_') AS plat,
  regexp_replace(query['WT.si_x'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','_') AS si_x,
  decode_url(regexp_replace(query['WT.si_n'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','_')) AS si_n,
  regexp_replace(query['WT.si_s'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','_') AS si_s,
  regexp_replace(query['WT.goods_id'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','_') AS goods_id,
  regexp_replace(query['WT.sku_id'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','_') AS sku_id,
  decode_url(query['WT.prepare1']) as prepare1,
  decode_url(query['WT.event_type']) AS event_type,
  decode_url(query['WT.envName']) AS envName,
  query['WT.errCode'] AS errCode,
  decode_url(query['WT.errMsg']) AS errMsg,
--  case when regexp_replace(query['WT.mc_ev'],'\\|','_')='221103_SJBHD'
--  then decode_url(query['WT.WT.prov']) else decode_url(query['WT.prov']) end as prov,
  decode_url(query['WT.prov']) AS prov,
  decode_url(query['WT.city']) AS city,
  decode_url(query['WT.mc_ev_name']) AS mc_ev_name,
  case   when dcsid ='3w7x' then 'H5'  
         when dcsid ='5m9b' or dcsid='7e9m' then '小程序'             
         when dcsid ='5i5n' then '公众号'                             
         when dcsid ='2r9o' then '2r9o'
         when dcsid ='3l2h' then '3l2h'
         when dcsid ='3s3i' then '3s3i'
         when dcsid ='7p1j' then '7p1j'
         when dcsid ='3a5k' then '3a5k'
         when dcsid ='8z2s' then '8z2s'
         when dcsid ='1w7q' then '1w7q'
         when dcsid ='9w6z' then '9w6z'
         when dcsid ='8f4q' then '8f4q'
         when dcsid ='2i1b' then '2i1b'
         when dcsid ='6g7b' then '6g7b'
         when dcsid ='0d4d' then '0d4d'
         when dcsid ='8d2o' then '8d2o'
         when dcsid ='8m4a' then '8m4a'
         when dcsid ='5d3k' then '5d3k'
         when dcsid ='3z9h' then '3z9h'
         when dcsid ='4w6p' then '4w6p'
         when dcsid ='5f8e' then '5f8e'
         when dcsid ='3j1d' then '3j1d'
         when dcsid ='9o0p' then '9o0p'
         when dcsid ='0o1j' then '0o1j'
         when dcsid ='3z7i' then '3z7i'
         when dcsid ='9b5g' then '9b5g'
         when dcsid ='3r8r' then '3r8r'
         when dcsid ='3d2m' then '3d2m'
         when dcsid ='9m2x' then '9m2x'
         when dcsid ='8o8q' then '8o8q'
         when dcsid ='2v6r' then '2v6r'
         when dcsid ='8j5v' then '8j5v'
         when dcsid ='4z6i' then '4z6i'
         when dcsid ='9y7c' then '9y7c'
         when dcsid ='8s0b' then '8s0b'
         when dcsid ='1g3e' then '1g3e'
         when dcsid ='4t5g' then '4t5g'
         when dcsid ='9o4w' then '9o4w'
         when dcsid ='3n9u' then '3n9u'
         when dcsid ='6c9w' then '6c9w'
         when dcsid ='2n3t' then '2n3t'
         when dcsid ='7c2d' then '7c2d'
         else null end
         as trmnl_style,
  query['WT.tbd'] AS tbd,
  query['WT.sr'] AS sr,
  query['WT.appId'] AS appid,
  query['WT.cid'] as cid,
  query['WT.userBrand'] as userbrand,
--  case when regexp_replace(query['WT.mc_ev'],'\\|','_')='221103_SJBHD' 
--  then decode_url(query['WT.WT.loginProvince']) else decode_url(query['WT.loginProvince']) end as loginprovince,
  query['WT.loginProvince'] as loginprovince,
  query['WT.loginCity'] as logincity,
  query['WT.nodeId'] as nodeid,
  query['WT.tv'] as tv,
  query['WT.vt_f_tlh'] as vt_f_tlh,
  query['WT.vtvs'] as vtvs,
  query['WT.branch'] as branch,
  query['dcsuri'] as dcsuri,
  decode_url(query['WT.ti']) as ti,
  query['WT.et'] as et,
  query['WT.markId'] as markid,
  query['WT.serial_no'] as serial_no,
  query['WT.act_str_step_id'] as act_str_step_id,
  query['WT.group_id'] as group_id,
  query['WT.mr_id'] as mr_id,
  query['WT.ad_step'] as ad_step,
  query['WT.touch_tm'] as touch_tm,
  query['WT.ed'] as ed,
  decode_url(query['WT.XY']) as xy,
  tb3.final_city_name,
  decode_url(query['WT.input_mobile']) as wt_input_mobile,
  decode_url(query['WT.goods_specs']) as wt_goods_specs,
  decode_url(query['WT.goods_price']) as wt_goods_price   
from ods_dcslog_delta tb1 left join dim_prov_code tb2 on parse_ip(case when upper(nullif(ip,'')) = 'NULL' or ip is null or ip = 'wap.js.10086.cn' then '' else ip end).prov=ip_prov left join dim_final_city tb3 on parse_ip(case when upper(nullif(ip,'')) = 'NULL' or ip is null or ip = 'wap.js.10086.cn' then '' else ip end).city=city_name
        where dt='${DT}'
          and hour = '${HH}'
  and daytime is not null
and dcsid in ('3w7x','5m9b','7e9m','5i5n','2r9o','3l2h','4t5g','9o4w','7c2d','3s3i','7p1j','3a5k','8z2s','1w7q','9w6z','8f4q','2i1b','6g7b','0d4d','8d2o','8m4a','5d3k','3z9h','4w6p','5f8e','3j1d','9o0p','0o1j','3z7i','9b5g','3r8r','3d2m','9m2x','8o8q','2v6r','8j5v','4z6i','9y7c','8s0b','1g3e','3n9u','6c9w','2n3t');
[root@api-core-1 h5]# cat dwd_jyh5_monitor_dh.hql 
set hive.default.fileformat = TextFile;
set hive.execution.engine = mr;
set hive.vectorized.execution.enabled  = false;

insert overwrite  table ham.dwd_monitor_dh_t partition (tp='jyh5',dt = '${DT}', hour = '${HH}')
select 'jyh5' as type
,dt as day
,hour as hh
,count(1) as count
from ham.dwd_dcslog_event_di
where dt = '${DT}'
and hour = '${HH}'
group by dt ,hour;
