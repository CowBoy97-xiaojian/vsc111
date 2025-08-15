set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

insert overwrite table ham_jituan.ads_hachi_jzyy_xtb2_51007_gio_dt_hour partition (dt = '${DT}', hour = '${HH}')
select
    regexp_replace(date_time,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as date_time,
    regexp_replace(c_ip,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as c_ip,
    regexp_replace(cs_host,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as cs_host,
    regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_cid,
    regexp_replace(wt_co_f,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_co_f,
    regexp_replace(wt_city,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_city,
    regexp_replace(wt_mobile,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_mobile,
    regexp_replace(wt_event,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_event,
    regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_ti,
    regexp_replace(wt_mc_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_mc_id,
    regexp_replace(wt_ac,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_ac,
    regexp_replace(wt_ac_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_ac_id,
    regexp_replace(goods_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as goods_id,
    regexp_replace(sku_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sku_id,
    regexp_replace(cs_referer,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as cs_referer,
    decode_url(regexp_replace(wt_es,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')) wt_es,
    regexp_replace(advertype,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as advertype,
    regexp_replace(wt_areaname,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_areaname,
    regexp_replace(mark_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mark_id,
    regexp_replace(trmnl_style,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as trmnl_style,
    regexp_replace(wt_aav,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_aav,
    regexp_replace(wt_av,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_av,
    regexp_replace(date_day,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as date_day,
    regexp_replace(prov_name,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prov_name,
    regexp_replace(click_time,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as click_time,
    regexp_replace(wt_prov, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_prov,
    regexp_replace(channelid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as channelid,
    regexp_replace(plat,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as plat,
    regexp_replace(si_x,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as si_x,
    regexp_replace(si_n,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as si_n,
    regexp_replace(si_s,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as si_s,
    regexp_replace(wt_goods_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_goods_id,
    regexp_replace(wt_sku_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_sku_id,
    regexp_replace(prepare1,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prepare1,
    regexp_replace(pageid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as pageid,
    regexp_replace(sellerid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sellerid,
    regexp_replace(extendid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as extendid,
    regexp_replace(wt_channel_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as wt_channel_id,
    regexp_replace(wt_page_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_page_id,
    regexp_replace(wt_seller_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_seller_id,
    regexp_replace(wt_extend_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_extend_id,
    regexp_replace(user_agent, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as user_agent,
    regexp_replace(cmd,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as cmd,
    regexp_replace(os,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as os,
    regexp_replace(dm, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as dm,
    regexp_replace(sv,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sv,
    decode_url(regexp_replace(a_dc, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')) as a_dc,
    regexp_replace(ct,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ct,
    regexp_replace(sr,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sr,
    regexp_replace(ss_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as vt_sid,
    regexp_replace(userbrand,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as userbrand,
    regexp_replace(loginprovince,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as loginprovince,
    regexp_replace(logincity,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as logincity,
    regexp_replace(appid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as appid,
    regexp_replace(channel,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as channel,
    regexp_replace(et, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as et,
    regexp_replace(ed,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ed,
    decode_url(regexp_replace(envname, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')) as envname,
    decode_url(regexp_replace(next_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')) as next_url,
    regexp_replace(errcode, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errcode,
    regexp_replace(errmsg,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errmsg,
    regexp_replace(serial_no,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as serial_no,
    regexp_replace(act_str_step_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as act_str_step_id,
    regexp_replace(group_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as group_id,
    regexp_replace(mr_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mr_id,
    regexp_replace(ad_step,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ad_step,
    regexp_replace(touch_tm,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as touch_tm,
    regexp_replace(appqry, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as appqry,
    regexp_replace(xy, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as xy,
    regexp_replace(sid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sid
from ham_jituan.dwd_client_event_gio_di 
where dt='${DT}'
and hour = '${HH}'
--20230925：增加domain--start--
--and trmnl_style in ('b95440ef47ec01fc','90be4403373b6463','a1f48d9ff4f42571','b508a809cbbddd0b')
and (
    trmnl_style in ('b95440ef47ec01fc','90be4403373b6463')
or
    (trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b')
    and wt_event in ('c_perf','FlowDataCollection')
    )
)
union all

select
    regexp_replace(date_time,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as date_time,
    regexp_replace(c_ip,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as c_ip,
    regexp_replace(cs_host,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as cs_host,
    regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_cid,
    regexp_replace(wt_co_f,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_co_f,
    regexp_replace(wt_city,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_city,
    regexp_replace(wt_mobile,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_mobile,
    regexp_replace(wt_event,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_event,
    regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_ti,
    regexp_replace(wt_mc_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_mc_id,
    regexp_replace(wt_ac,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_ac,
    regexp_replace(wt_ac_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_ac_id,
    regexp_replace(goods_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as goods_id,
    regexp_replace(sku_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sku_id,
    regexp_replace(cs_referer,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as cs_referer,
    decode_url(regexp_replace(wt_es,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')) wt_es,
    regexp_replace(advertype,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as advertype,
    regexp_replace(wt_areaname,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_areaname,
    regexp_replace(mark_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mark_id,
    regexp_replace(trmnl_style,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as trmnl_style,
    regexp_replace(wt_aav,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_aav,
    regexp_replace(wt_av,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_av,
    regexp_replace(date_day,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as date_day,
    regexp_replace(prov_name,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prov_name,
    regexp_replace(click_time,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as click_time,
    regexp_replace(wt_prov, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_prov,
    regexp_replace(channelid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as channelid,
    regexp_replace(plat,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as plat,
    regexp_replace(si_x,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as si_x,
    regexp_replace(si_n,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as si_n,
    regexp_replace(si_s,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as si_s,
    regexp_replace(wt_goods_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_goods_id,
    regexp_replace(wt_sku_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_sku_id,
    regexp_replace(prepare1,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as prepare1,
    regexp_replace(pageid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as pageid,
    regexp_replace(sellerid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sellerid,
    regexp_replace(extendid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as extendid,
    regexp_replace(wt_channel_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as wt_channel_id,
    regexp_replace(wt_page_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_page_id,
    regexp_replace(wt_seller_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_seller_id,
    regexp_replace(wt_extend_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as wt_extend_id,
    regexp_replace(user_agent, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as user_agent,
    regexp_replace(cmd,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as cmd,
    regexp_replace(os,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as os,
    regexp_replace(dm, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as dm,
    regexp_replace(sv,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sv,
    decode_url(regexp_replace(a_dc, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')) as a_dc,
    regexp_replace(ct,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ct,
    regexp_replace(sr,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sr,
    regexp_replace(vt_sid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as vt_sid,
    regexp_replace(userbrand,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as userbrand,
    regexp_replace(loginprovince,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as loginprovince,
    regexp_replace(logincity,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as logincity,
    regexp_replace(appid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as appid,
    regexp_replace(channel,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as channel,
    regexp_replace(et, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as et,
    regexp_replace(ed,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ed,
    decode_url(regexp_replace(envname, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')) as envname,
    decode_url(regexp_replace(next_url, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')) as next_url,
    regexp_replace(errcode, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errcode,
    regexp_replace(errmsg,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as errmsg,
    regexp_replace(serial_no,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as serial_no,
    regexp_replace(act_str_step_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as act_str_step_id,
    regexp_replace(group_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as group_id,
    regexp_replace(mr_id,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as mr_id,
    regexp_replace(ad_step,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as ad_step,
    regexp_replace(touch_tm,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as touch_tm,
    regexp_replace(appqry, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as appqry,
    regexp_replace(xy, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as xy,
    regexp_replace(sid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '') as sid
from ham_jituan.dwd_client_event_gio_di tb2
inner join (
select
domain,
interface
from ham.dim_domain_interface_di
where interface = '51007') tb4
on tb2.domain=tb4.domain
where dt='${DT}'
and hour = '${HH}'
and trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b')
--20230925：增加domain--end--
;


