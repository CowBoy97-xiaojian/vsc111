set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

INSERT OVERWRITE TABLE ham.ads_rpt_hachi_51006_gio_hourly partition (dt = '${DT}', hour = '${HH}')
select
regexp_replace(daytime, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as daytime,
regexp_replace(ip, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip,
regexp_replace(channel_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as channel_id,
regexp_replace(page_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as page_id,
regexp_replace(seller_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as seller_id,
regexp_replace(dcsdat, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtdcsdat,
regexp_replace(wtchannelid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtchannelid,
regexp_replace(ck_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtco_f,
regexp_replace(event, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtevent,
regexp_replace(es, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wturl,
regexp_replace(mc_ev, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtmc_ev,
regexp_replace(wtsellerid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtsellerid,
regexp_replace(wtpageid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtpageid,
regexp_replace(last_pageid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtlast_pageid,
regexp_replace(last_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtlast_url,
regexp_replace(last_pagename, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtlast_pagename,
regexp_replace(current_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtcurrent_url,
regexp_replace(current_pagename, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtcurrent_pagename,
regexp_replace(module_no, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtmodule_no,
regexp_replace(module_name, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtmodule_name,
regexp_replace(point_position, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtpoint_position,
regexp_replace(member, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtmember,
regexp_replace(goods_no, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtgoods_no,
regexp_replace(login_status, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtlogin_status,
regexp_replace(next_pageid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtnext_pageid,
regexp_replace(next_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtnext_url,
regexp_replace(next_pagename, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtnext_pagename,
regexp_replace(component_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtcomponent_id,
regexp_replace(user_agent, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as user_agent,
regexp_replace(referer, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as referer,
regexp_replace(mobile, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mobile,
regexp_replace(ss_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as session_id,
'--',
regexp_replace(environment, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as environment,
regexp_replace(code, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip_prov,--根据ip解析，关联维表，省份代码
regexp_replace(plat, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as plat,
regexp_replace(si_x, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_x,
regexp_replace(si_n, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_n,
regexp_replace(si_s, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_s,
regexp_replace(goods_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as goods_id,
regexp_replace(sku_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sku_id,
regexp_replace(prepare1, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prepare1,
regexp_replace(event_type, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as event_type,
regexp_replace(envname, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as envname,
regexp_replace(errcode, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errcode,
regexp_replace(errmsg, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errmsg,
regexp_replace(prov, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prov,
regexp_replace(city, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as city,
regexp_replace(mc_ev_name, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mc_ev_name,
regexp_replace(trmnl_style, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as trmnl_style,
regexp_replace(tbd, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as tbd,
regexp_replace(sr, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sr,
regexp_replace(appid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as appid,
regexp_replace(cid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as cid,
regexp_replace(userbrand, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as userbrand,
regexp_replace(loginprovince, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as loginprovince,
regexp_replace(logincity, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as logincity,
regexp_replace(nodeid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as nodeid,
regexp_replace(tv, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as tv,
regexp_replace(vt_f_tlh, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as vt_f_tlh,
regexp_replace(vtvs, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as vtvs,
regexp_replace(branch, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as branch,
regexp_replace(dcsuri, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as dcsuri,
regexp_replace(ti, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ti,
regexp_replace(et, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as et,
regexp_replace(markid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as markid,
regexp_replace(serial_no, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as serial_no,
regexp_replace(act_str_step_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as act_str_step_id,
regexp_replace(group_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as group_id,
regexp_replace(mr_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mr_id,
regexp_replace(ad_step, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ad_step,
regexp_replace(touch_tm, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as touch_tm,
regexp_replace(ed, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ed,
regexp_replace(xy, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as xy,
regexp_replace(tb3.final_city_name, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip_city,--根据ip解析，关联维表，格式化后的城市中文名称
regexp_replace(input_mobile, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_input_mobile,
regexp_replace(wt_goods_specs, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_goods_specs,
regexp_replace(wt_goods_price, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_goods_price
from ham.dwd_dcslog_event_gio_di tb1 
left join ham.dim_prov_code tb2 
on tb1.ip_prov=tb2.ip_prov 
left join ham.dim_final_city tb3 
on tb1.ip_city=tb3.city_name
where dt = '${DT}'
and hour = '${HH}'
and trmnl_style in ('ac81a55cdf7074d5','aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2','999264cd2e773538','beb5471c243b2971','b5b9ccb1f69a35b9')
;
--融通部分
INSERT INTO TABLE ham.ads_rpt_hachi_51006_gio_hourly partition (dt = '${DT}', hour = '${HH}')
select 
regexp_replace(daytime, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as daytime,
regexp_replace(ip, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip,
regexp_replace(channel_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as channel_id,
regexp_replace(page_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as page_id,
regexp_replace(seller_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as seller_id,
regexp_replace(dcsdat, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtdcsdat,
regexp_replace(wtchannelid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtchannelid,
regexp_replace(ck_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtco_f,
regexp_replace(event, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtevent,
regexp_replace(es, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wturl,
regexp_replace(mc_ev, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtmc_ev,
regexp_replace(wtsellerid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtsellerid,
regexp_replace(wtpageid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtpageid,
regexp_replace(last_pageid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtlast_pageid,
regexp_replace(last_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtlast_url,
regexp_replace(last_pagename, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtlast_pagename,
regexp_replace(current_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtcurrent_url,
regexp_replace(current_pagename, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtcurrent_pagename,
regexp_replace(module_no, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtmodule_no,
regexp_replace(module_name, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtmodule_name,
regexp_replace(point_position, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtpoint_position,
regexp_replace(member, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtmember,
regexp_replace(goods_no, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtgoods_no,
regexp_replace(login_status, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtlogin_status,
regexp_replace(next_pageid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtnext_pageid,
regexp_replace(next_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtnext_url,
regexp_replace(next_pagename, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtnext_pagename,
regexp_replace(component_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtcomponent_id,
regexp_replace(user_agent, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as user_agent,
regexp_replace(referer, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as referer,
regexp_replace(mobile, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mobile,
regexp_replace(ss_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as session_id,
'--',
regexp_replace(environment, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as environment,
regexp_replace(code, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip_prov,--根据ip解析，关联维表，省份代码
regexp_replace(plat, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as plat,
regexp_replace(si_x, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_x,
regexp_replace(si_n, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_n,
regexp_replace(si_s, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_s,
regexp_replace(goods_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as goods_id,
regexp_replace(sku_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sku_id,
regexp_replace(prepare1, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prepare1,
regexp_replace(event_type, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as event_type,
regexp_replace(envname, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as envname,
regexp_replace(errcode, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errcode,
regexp_replace(errmsg, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errmsg,
regexp_replace(prov, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prov,
regexp_replace(city, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as city,
regexp_replace(mc_ev_name, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mc_ev_name,
regexp_replace(trmnl_style, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as trmnl_style,
regexp_replace(tbd, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as tbd,
regexp_replace(sr, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sr,
regexp_replace(appid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as appid,
regexp_replace(cid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as cid,
regexp_replace(userbrand, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as userbrand,
regexp_replace(loginprovince, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as loginprovince,
regexp_replace(logincity, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as logincity,
regexp_replace(nodeid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as nodeid,
regexp_replace(tv, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as tv,
regexp_replace(vt_f_tlh, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as vt_f_tlh,
regexp_replace(vtvs, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as vtvs,
regexp_replace(branch, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as branch,
regexp_replace(dcsuri, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as dcsuri,
regexp_replace(ti, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ti,
regexp_replace(et, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as et,
regexp_replace(markid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as markid,
regexp_replace(serial_no, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as serial_no,
regexp_replace(act_str_step_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as act_str_step_id,
regexp_replace(group_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as group_id,
regexp_replace(mr_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mr_id,
regexp_replace(ad_step, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ad_step,
regexp_replace(touch_tm, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as touch_tm,
regexp_replace(ed, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ed,
regexp_replace(xy, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as xy,
regexp_replace(tb3.final_city_name, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip_city,--根据ip解析，关联维表，格式化后的城市中文名称
regexp_replace(input_mobile, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_input_mobile,
regexp_replace(wt_goods_specs, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_goods_specs,
regexp_replace(wt_goods_price, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_goods_price
from ham.dwd_dcslog_event_gio_di tb1 
inner join (
select
domain,
interface
from ham.dim_domain_interface_di
where interface = '51006') tb4 
on tb1.domain=tb4.domain
left join ham.dim_prov_code tb2 
on tb1.ip_prov=tb2.ip_prov 
left join ham.dim_final_city tb3 
on tb1.ip_city=tb3.city_name 
where dt = '${DT}'
and hour = '${HH}'
and trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b')
;


