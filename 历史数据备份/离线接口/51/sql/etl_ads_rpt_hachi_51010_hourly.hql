set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

INSERT OVERWRITE TABLE ham.ads_rpt_hachi_51010_hourly partition (dt = '${DT}', hour = '${HH}')
SELECT  regexp_replace(daytime, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as daytime
       ,regexp_replace(ip, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip
       ,regexp_replace(wtdcsdat, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtdcsdat
       ,regexp_replace(wtchannelid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtchannelid
       ,regexp_replace(wtco_f, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtco_f
       ,regexp_replace(session_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_vt_sid
       ,regexp_replace(wtevent, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtevent
       ,regexp_replace(wturl, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wturl
       ,regexp_replace(wtmc_ev, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtmc_ev
       ,regexp_replace(wtnext_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtnext_url
       ,regexp_replace(user_agent, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as user_agent
       ,regexp_replace(mobile, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mobile
       ,regexp_replace(si_x, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_x
       ,regexp_replace(si_n, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_n
       ,regexp_replace(si_s, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_s
       ,regexp_replace(goods_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as goods_id
       ,regexp_replace(sku_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sku_id
       ,regexp_replace(envname, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as envname
       ,regexp_replace(errcode, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errcode
       ,regexp_replace(errmsg, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errmsg
       ,regexp_replace(prov, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prov
       ,regexp_replace(city, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as city
       ,regexp_replace(trmnl_style, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as trmnl_style
       ,regexp_replace(sr, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sr
       ,regexp_replace(appid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as appid
       ,regexp_replace(cid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as cid
       ,regexp_replace(userbrand, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as userbrand
       ,regexp_replace(loginprovince, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as loginprovince
       ,regexp_replace(logincity, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as logincity
       ,regexp_replace(nodeid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as nodeid
       ,regexp_replace(tv, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as tv
       ,regexp_replace(vt_f_tlh, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as vt_f_tlh
       ,regexp_replace(vtvs, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as vtvs
       ,regexp_replace(branch, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as branch
       ,regexp_replace(dcsuri, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as dcsuri
       ,regexp_replace(ti, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ti
       ,regexp_replace(et, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as et
       ,regexp_replace(markid, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as markid
       ,regexp_replace(serial_no, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as serial_no
       ,regexp_replace(act_str_step_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as act_str_step_id
       ,regexp_replace(group_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as group_id
       ,regexp_replace(mr_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mr_id
       ,regexp_replace(ad_step, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ad_step
       ,regexp_replace(touch_tm, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as touch_tm
       ,regexp_replace(ed, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ed
       ,regexp_replace(xy, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as xy
       ,regexp_replace(prepare1, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prepare1
       ,'--' as event_id
       ,'--' as event_type
       ,'--' as user_key
       ,regexp_replace(parse_url(wturl,'HOST'), '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as domain
       ,'--' as device_model
       ,'--' as sdk_version
       ,'--' as location_latitude
       ,'--' as location_longitude
       ,'--' as platform
       ,'--' as os_version
       ,'--' as gio_id
       ,regexp_replace(adddataxy(if(xy is null,'',xy)), '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as attributes
       ,regexp_replace(ip_prov, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip_prov
       ,regexp_replace(ip_city, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip_city
FROM ham.ads_rpt_hachi_51006_dt_hour
WHERE dt = '${DT}'
and hour = '${HH}'
and trmnl_style in ('7c2d','3s3i','7p1j','3a5k','8z2s','1w7q','9w6z','8f4q','2i1b','6g7b','0d4d','8d2o','8m4a','5d3k','3z9h','4w6p','5f8e','3j1d','9o0p','0o1j','3z7i','9b5g','3r8r','3d2m','9m2x','8o8q','2v6r','8j5v','4z6i','9y7c','8s0b','1g3e','3n9u','6c9w','2n3t')
;

INSERT INTO TABLE ham.ads_rpt_hachi_51010_hourly partition (dt = '${DT}', hour = '${HH}')
select
regexp_replace(daytime, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as daytime,
regexp_replace(ip, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip,
regexp_replace(dcsdat, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as dcsdat,
regexp_replace(channel_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as channel_id,
regexp_replace(ck_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtco_f,
regexp_replace(ss_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_vt_sid,
regexp_replace(event, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtevent,
regexp_replace(es, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wturl,
regexp_replace(mc_ev, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtmc_ev,
regexp_replace(next_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtnext_url,
regexp_replace(user_agent, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as user_agent,
regexp_replace(mobile, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mobile,
regexp_replace(si_x, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_x,
regexp_replace(si_n, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_n,
regexp_replace(si_s, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_s,
regexp_replace(goods_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as goods_id,
regexp_replace(sku_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sku_id,
regexp_replace(envname, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as envname,
regexp_replace(errcode, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errcode,
regexp_replace(errmsg, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errmsg,
regexp_replace(prov, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prov,
regexp_replace(city, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as city,
regexp_replace(trmnl_style, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as trmnl_style,
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
regexp_replace(prepare1, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prepare1,
regexp_replace(event_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as event_id,
regexp_replace(event_type, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as event_type,
regexp_replace(user_key, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as user_key,
regexp_replace(domain, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as domain,
regexp_replace(device_model, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as device_model,
regexp_replace(sdk_version, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sdk_version,
'--',
'--',
regexp_replace(platform, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as platform,
regexp_replace(os_version, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as os_version,
regexp_replace(gio_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as gio_id,
regexp_replace(attributes, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as attributes,
regexp_replace(code, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip_prov,--根据ip解析，关联维表，省份代码
regexp_replace(tb3.final_city_name, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip_city --根据ip解析，关联维表，格式化后的城市中文名称
from ham.dwd_dcslog_event_gio_di tb1 
left join ham.dim_prov_code tb2 
on tb1.ip_prov=tb2.ip_prov 
left join ham.dim_final_city tb3 
on tb1.ip_city=tb3.city_name
where dt = '${DT}'
and hour = '${HH}'
and trmnl_style in ('aae6afbf8823b1c1','a797613d647af6c6','a7464be8b200fe24','b9e74b9d5da046ce','b87eb8cec01051a6','b098587a3af15a25','b23efeab1461679b','a10b1f67c7d288c1','ae2308d5a08258b6','a04f0ad9925c080f','a6ba03d6884ee1c1','aa3a7b170aa41eed','a441e7427a44d390','adae9b8274a853f1','ad2e89b1010aed1e','bfe72e945deea64f','81ebc780373ee7cd','8d5d4407d1c3f6bf','abd580f08cf381e0','ba81a7647a8bed5d','8c19b47f237e28d3','a609de73d1690c0e','9c294cf845907ac1','b0839305cf5d280d','9a238b92b6612604','afdbd0f6b9a7ba30','8273883304ad70fb','86fdf7160c5cc8cc','81fab8d01c17ee89','89e7161d4a6b817f','9b50668a016469ec','bda85eeb52fb8b87','a8c29f7abfd639b4','b8d9fd437de471e4','875444ff6e049f33','853d176d98834573')
;

INSERT INTO TABLE ham.ads_rpt_hachi_51010_hourly partition (dt = '${DT}', hour = '${HH}')
select
regexp_replace(daytime, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as daytime,
regexp_replace(ip, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip,
regexp_replace(dcsdat, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as dcsdat,
regexp_replace(channel_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as channel_id,
regexp_replace(ck_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtco_f,
regexp_replace(ss_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_vt_sid,
regexp_replace(event, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtevent,
regexp_replace(es, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wturl,
regexp_replace(mc_ev, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtmc_ev,
regexp_replace(next_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wtnext_url,
regexp_replace(user_agent, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as user_agent,
regexp_replace(mobile, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mobile,
regexp_replace(si_x, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_x,
regexp_replace(si_n, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_n,
regexp_replace(si_s, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as si_s,
regexp_replace(goods_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as goods_id,
regexp_replace(sku_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sku_id,
regexp_replace(envname, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as envname,
regexp_replace(errcode, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errcode,
regexp_replace(errmsg, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errmsg,
regexp_replace(prov, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prov,
regexp_replace(city, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as city,
regexp_replace(trmnl_style, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as trmnl_style,
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
regexp_replace(prepare1, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prepare1,
regexp_replace(event_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as event_id,
regexp_replace(event_type, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as event_type,
regexp_replace(user_key, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as user_key,
regexp_replace(tb1.domain, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as domain,
regexp_replace(device_model, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as device_model,
regexp_replace(sdk_version, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sdk_version,
'--',
'--',
regexp_replace(platform, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as platform,
regexp_replace(os_version, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as os_version,
regexp_replace(gio_id, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as gio_id,
regexp_replace(attributes, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as attributes,
regexp_replace(code, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip_prov,--根据ip解析，关联维表，省份代码
regexp_replace(tb3.final_city_name, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ip_city --根据ip解析，关联维表，格式化后的城市中文名称
from ham.dwd_dcslog_event_gio_di tb1 
inner join (
select
domain,
interface
from ham.dim_domain_interface_di
where interface = '51010') tb4 
on tb1.domain=tb4.domain
left join ham.dim_prov_code tb2 
on tb1.ip_prov=tb2.ip_prov 
left join ham.dim_final_city tb3 
on tb1.ip_city=tb3.city_name
where dt = '${DT}'
and hour = '${HH}'
and trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b')
;
