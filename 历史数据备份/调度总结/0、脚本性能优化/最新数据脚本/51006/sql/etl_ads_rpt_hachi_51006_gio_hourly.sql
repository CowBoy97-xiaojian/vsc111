set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

INSERT OVERWRITE TABLE ham.ads_rpt_hachi_51006_gio_hourly partition (dt = '${DT}', hour = '${HH}')
select
daytime as daytime,
ip as ip,
channel_id as channel_id,
page_id as page_id,
seller_id as seller_id,
dcsdat as wtdcsdat,
wtchannelid as wtchannelid,
ck_id as wtco_f,
event as wtevent,
regexp_replace(es, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wturl,
mc_ev as wtmc_ev,
wtsellerid as wtsellerid,
wtpageid as wtpageid,
last_pageid as wtlast_pageid,
regexp_replace(last_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtlast_url,
last_pagename as wtlast_pagename,
regexp_replace(current_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtcurrent_url,
current_pagename as wtcurrent_pagename,
module_no as wtmodule_no,
module_name as wtmodule_name,
point_position as wtpoint_position,
member as wtmember,
goods_no as wtgoods_no,
login_status as wtlogin_status,
next_pageid as wtnext_pageid,
regexp_replace(next_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtnext_url,
next_pagename as wtnext_pagename,
component_id as wtcomponent_id,
regexp_replace(user_agent, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as user_agent,
referer as referer,
mobile as mobile,
ss_id as session_id,
input_sfz as wtinput_sfz,
environment as environment,
code as ip_prov,--根据ip解析，关联维表，省份代码
plat as plat,
regexp_replace(si_x, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_x,
regexp_replace(si_n, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_n,
regexp_replace(si_s, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_s,
goods_id as goods_id,
sku_id as sku_id,
prepare1 as prepare1,
event_type as event_type,
regexp_replace(envname, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as envname,
regexp_replace(errcode, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errcode,
regexp_replace(errmsg, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errmsg,
prov as prov,
city as city,
mc_ev_name as mc_ev_name,
trmnl_style as trmnl_style,
tbd as tbd,
sr as sr,
appid as appid,
regexp_replace(cid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as cid,
userbrand as userbrand,
loginprovince as loginprovince,
logincity as logincity,
nodeid as nodeid,
tv as tv,
vt_f_tlh as vt_f_tlh,
vtvs as vtvs,
branch as branch,
regexp_replace(dcsuri, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as dcsuri,
regexp_replace(ti, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ti,
et as et,
markid as markid,
serial_no as serial_no,
act_str_step_id as act_str_step_id,
group_id as group_id,
mr_id as mr_id,
ad_step as ad_step,
touch_tm as touch_tm,
ed as ed,
xy as xy,
tb3.final_city_name as ip_city,--根据ip解析，关联维表，格式化后的城市中文名称
input_mobile as wt_input_mobile,
wt_goods_specs as wt_goods_specs,
wt_goods_price as wt_goods_price
from ham.dwd_dcslog_event_gio_di tb1 
left join ham.dim_prov_code tb2 
on tb1.ip_prov=tb2.ip_prov 
left join ham.dim_final_city tb3 
on tb1.ip_city=tb3.city_name
where dt = '${DT}'
and hour = '${HH}'
--and trmnl_style in ('aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2','a1f48d9ff4f42571','b508a809cbbddd0b')
and (trmnl_style in ('ac81a55cdf7074d5','aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2')
or (trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b') and (
-- instr(mc_ev,'QYCS')<0 
-- or mc_ev is null
-- --or (domain = 'dev.coc.10086.cn' and attributes['WT_environment'] = 'production' and attributes['$path'] like '/coc3/canvas/rightsmarket-h5-canvas%')
-- or (environment = 'production' 
-- and url like 'https://dev.coc.10086.cn/coc3/canvas/rightsmarket-h5-canvas%')
-- )))
instr(mc_ev,'QYCS')=0 
or mc_ev is null
or mc_ev = '210315_QYCS'
--or (domain = 'dev.coc.10086.cn' and attributes['WT_environment'] = 'production' and attributes['$path'] like '/coc3/canvas/rightsmarket-h5-canvas%')
or  url like 'https://dev.coc.10086.cn/coc3/canvas/rightsmarket-h5-canvas%'
)))
;
