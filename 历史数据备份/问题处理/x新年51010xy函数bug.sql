select 
wt_xy,
adddataxy(if(wt_xy is null,'',wt_xy))
from ads_rpt_hachi_51010_hourly where wt_xy is not null limit 1;

select
adddataxy(if(wt_xy is null,'',wt_xy))
from ham.dwd_dcslog_event_gio_di 
limit 1;

select
daytime as daytime,
ip as ip,
dcsdat as dcsdat,
channel_id as channel_id,
ck_id as wtco_f,
ss_id as wt_vt_sid,
event as wtevent,
es as wturl,
mc_ev as wtmc_ev,
next_url as wtnext_url,
user_agent as user_agent,
mobile as mobile,
si_x as si_x,
si_n as si_n,
si_s as si_s,
goods_id as goods_id,
sku_id as sku_id,
envname as envname,
errcode as errcode,
errmsg as errmsg,
prov as prov,
city as city,
trmnl_style as trmnl_style,
sr as sr,
appid as appid,
cid as cid,
userbrand as userbrand,
loginprovince as loginprovince,
logincity as logincity,
nodeid as nodeid,
tv as tv,
vt_f_tlh as vt_f_tlh,
vtvs as vtvs,
branch as branch,
dcsuri as dcsuri,
ti as ti,
et as et,
markid as markid,
serial_no as serial_no,
act_str_step_id as act_str_step_id,
group_id as group_id,
mr_id as mr_id,
ad_step as ad_step,
touch_tm as touch_tm,
ed as ed,
--xy as xy,
adddataxy(if(xy is null,'',xy)),
prepare1 as prepare1,
event_id as event_id,
event_type as event_type,
user_key as user_key,
domain as domain,
device_model as device_model,
sdk_version as sdk_version,
location_latitude as location_latitude,
location_longitude as location_longitude,
platform as platform,
os_version as os_version,
gio_id as gio_id,
attributes as attributes,
code as ip_prov,--根据ip解析，关联维表，省份代码
tb3.final_city_name as ip_city --根据ip解析，关联维表，格式化后的城市中文名称
from ham.dwd_dcslog_event_gio_di tb1 
left join ham.dim_prov_code tb2 
on tb1.ip_prov=tb2.ip_prov 
left join ham.dim_final_city tb3 
on tb1.ip_city=tb3.city_name
where trmnl_style in ('a797613d647af6c6','a7464be8b200fe24','b9e74b9d5da046ce','b87eb8cec01051a6','b098587a3af15a25','b23efeab1461679b','a10b1f67c7d288c1','ae2308d5a08258b6','a04f0ad9925c080f','a6ba03d6884ee1c1','aa3a7b170aa41eed','a441e7427a44d390','adae9b8274a853f1','ad2e89b1010aed1e','bfe72e945deea64f','81ebc780373ee7cd','8d5d4407d1c3f6bf','abd580f08cf381e0','ba81a7647a8bed5d','8c19b47f237e28d3','a609de73d1690c0e','9c294cf845907ac1','b0839305cf5d280d','9a238b92b6612604','afdbd0f6b9a7ba30','8273883304ad70fb','86fdf7160c5cc8cc','81fab8d01c17ee89','89e7161d4a6b817f','9b50668a016469ec','bda85eeb52fb8b87','a8c29f7abfd639b4','b8d9fd437de471e4','875444ff6e049f33','853d176d98834573') limit 1
;