select
cs_host,wt_mc_id,wt_ac,sku_id,cs_referer,wt_areaname,wt_aav,wt_av,prov_name,click_time,channelid,plat,prepare1,cmd,os,dm,sellerid,wt_page_id,wt_seller_id,wt_extend_id,user_agent,sv,carrieroperator,ct,sr,vt_sid,userbrand,dcsref,ed,errcode,errmsg,serial_no,act_str_step_id,group_id,mr_id,ad_step,touch_tm,appqry,xy,sid,dcsdat,event_id,sdk_version,platform,os_version,gio_id
,dt
,hour
,dcsid
from webtrends.event_hi_client_all 
where dt = '2023-04-25'
and (
cs_host
--<> '--' or wt_mc_id
--<> '--' or wt_ac
--<> '--' or sku_id
--<> '--' or cs_referer
<> '--' or wt_areaname
--<> '--' or wt_aav
--<> '--' or wt_av
<> '--' or prov_name
--<> '--' or click_time
--<> '--' or channelid
--<> '--' or plat
--<> '--' or wt_sku_id
--<> '--' or prepare1
--<> '--' or cmd
--<> '--' or os
--<> '--' or dm
--<> '--' or sellerid
--<> '--' or wt_page_id
--<> '--' or wt_seller_id
--<> '--' or wt_extend_id
<> '--' or user_agent
--<> '--' or sv
<> '--' or carrieroperator
--<> '--' or ct
--<> '--' or sr
--<> '--' or vt_sid
<> '--' or userbrand
--<> '--' or dcsref
--<> '--' or ed
--<> '--' or errcode
--<> '--' or errmsg
--<> '--' or serial_no
<> '--' or act_str_step_id
<> '--' or group_id
<> '--' or mr_id
--<> '--' or ad_step
<> '--' or touch_tm
--<> '--' or appqry
--<> '--' or xy
--<> '--' or sid
--<> '--' or dcsdat
<> '--' or event_id
<> '--' or sdk_version
<> '--' or platform
<> '--' or os_version
<> '--' or gio_id
<> '--'
)
limit 20
FORMAT CSVWithNames
;