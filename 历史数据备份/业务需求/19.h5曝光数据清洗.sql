#!/bin/sh
DT=$1
Hour=$2

clickhouse-client -h 10.253.248.73 -n --receive_timeout=3600 --query="
insert into table webtrends.event_hi_client_new_all
select 
date_time,
c_ip,
cs_host,
wt_cid,
wt_co_f,
wt_city,
wt_mobile,
wt_event,
wt_ti,
wt_mc_id,
wt_ac,
wt_ac_id,
goods_id,
sku_id,
cs_referer,
wt_es,
advertype,
wt_areaname,
mark_id,
trmnl_style,
wt_aav,
wt_av,
date_day,
prov_name,
click_time,
wt_prov,
channelid,
plat,
si_x,
si_n,
si_s,
wt_goods_id,
wt_sku_id,
prepare1,
cmd,
os,
dm,
clientid,
pageid,
sellerid,
ss_id,
extendid,
wt_channel_id,
wt_page_id,
wt_seller_id,
wt_extend_id,
user_agent,
et,
next_url,
envname,
sv,
carrieroperator,
a_dc,
ct,
sr,
vt_sid,
userbrand,
loginprovince,
logincity,
appid,
channel,
dcsref,
ed,
errcode,
errmsg,
serial_no,
act_str_step_id,
group_id,
mr_id,
ad_step,
touch_tm,
appqry,
xy,
sid,
dcsdat,
event_id,
event_type,
user_key,
domain,
device_model,
sdk_version,
location_latitude,
location_longitude,
platform,
os_version,
gio_id,
attributes,
area_id,
imp_type,
dt,
hour,
dcsid
from webtrends.evevent_hi_client_imp_all 
where 
dt = '${DT}'
and hour = '${Hour}'
dcsid = 'b95440ef47ec01fc';

event_hi_client_new_all

ck_client_flink_b95440ef47ec01fc_imp.sh


etl_ck_client_flink_b95440ef47ec01fc_imp = BashOperator(
        task_id="etl_ck_client_flink_b95440ef47ec01fc_imp",
        bash_command=f"""
        sleep 180s
        sh /home/udbac/hqls_ck/jituan/ck_client_flink_b95440ef47ec01fc_imp.sh {DT} {HH}
        """  ,
        dag=dag,
    )