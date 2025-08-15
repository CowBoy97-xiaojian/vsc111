insert into table webtrends.event_hi_dcslog_update_all
select 
coalesce(case when match(tb1.user, '^[0-9]{11}$') then tb1.user when match(tb1.user, '.*==$') then aes_function(tb1.user) end,tb2.mobile) as mobile
,tb1.ck_id
,tb1.ss_id
,tb1.ip
,tb1.ip_prov
,tb1.ip_city
,tb1.referer
,tb1.event
,tb1.url
,tb1.si_n
,tb1.si_s
,tb1.si_x
,tb1.channel_id
,tb1.page_id
,tb1.input_xm
,tb1.input_sfz
,tb1.input_dz
,tb1.mc_ev
,tb1.seller_id
,tb1.last_pageid
,tb1.module_no
,tb1.point_position
,tb1.member
,tb1.goods_no
,tb1.login_status
,tb1.input_dq
,tb1.input_buymobile
,tb1.daytime
,tb1.cli_time
,tb1.last_url
,tb1.last_pagename
,tb1.current_url
,tb1.current_pagename
,tb1.module_name
,tb1.next_pageid
,tb1.next_url
,tb1.next_pagename
,tb1.component_id
,tb1.input_mobile
,tb1.touchcode
,tb1.tpl
,tb1.branch
,tb1.complete_url
,tb1.point_name
,tb1.component_name
,tb1.environment
,tb1.plat
,tb1.sku_id
,tb1.prepare1
,tb1.event_type
,tb1.envname
,tb1.errcode
,tb1.errmsg
,tb1.prov
,tb1.city
,tb1.mc_ev_name
,tb1.trmnl_style
,tb1.tbd
,tb1.dcsdat
,tb1.user_agent
,tb1.goods_id
,tb1.sr
,tb1.appid
,tb1.cid
,tb1.userbrand
,tb1.loginprovince
,tb1.logincity
,tb1.nodeid
,tb1.tv
,tb1.vt_f_tlh
,tb1.vtvs
,tb1.mark33
,tb1.dcsuri
,tb1.wt_ti
,tb1.et
,tb1.markid
,tb1.serial_no
,tb1.act_str_step_id
,tb1.group_id
,tb1.mr_id
,tb1.ad_step
,tb1.touch_tm
,tb1.ed
,tb1.xy
,tb1.vt_sid
,tb1.es
,tb1.wtsellerid
,tb1.wtchannelid
,tb1.wtpageid
,tb1.event_id
,tb1.user_key
,tb1.domain
,tb1.device_model
,tb1.sdk_version
,tb1.location_latitude
,tb1.location_longitude
,tb1.platform
,tb1.os_version
,tb1.gio_id
,tb1.attributes
,tb1.dt
,tb1.hour
,tb1.dcsid
from
(
select
user,
anonymous_user,
attributes['ck_id'] AS ck_id,
decodeURLComponent(concat(attributes['ck_id'],':',attributes['WT_vtvs']))  AS ss_id,
ip,
splitByString('|',ip2region(ip))[1] as ip_prov,
splitByString('|',ip2region(ip))[2] as ip_city,
replaceRegexpAll(decodeURLComponent(decodeURLComponent(attributes['cs_referer'])), '\\s+', '') as referer,
decodeURLComponent(attributes['WT_event']) AS event,
splitByString('?',ifNull(decodeURLComponent(attributes['WT_es']),''))[1] as url,
decodeURLComponent(decodeURLComponent(attributes['WT_si_n'])) AS si_n,
decodeURLComponent(attributes['WT_si_s']) AS si_s,
decodeURLComponent(attributes['WT_si_x']) AS si_x,
if(wtchannelid = '--',attributes['channelId'],wtchannelid) as channel_id,
if(wtpageid = '--',attributes['pageId'],wtpageid) as page_id,
decodeURLComponent(attributes['WT_input_xm']) AS input_xm,
decodeURLComponent(attributes['WT_input_sfz'])  AS input_sfz,
decodeURLComponent(attributes['WT_input_dz']) AS input_dz,
decodeURLComponent(attributes['WT_mc_ev']) AS mc_ev,
if(wtsellerid ='--',attributes['sellerId'],wtsellerid) as seller_id,
attributes['WT_last_pageid'] AS last_pageid,
attributes['WT_module_no'] AS module_no,
decodeURLComponent(attributes['WT_point_position']) AS point_position,
decodeURLComponent(attributes['WT_member']) AS member,
decodeURLComponent(attributes['WT_goods_no']) AS goods_no,
attributes['WT_login_status'] AS login_status,
decodeURLComponent(attributes['WT_input_dq']) AS input_dq,
decodeURLComponent(attributes['WT_input_buymobile']) AS input_buymobile,
toDateTime(event_time) as daytime,
ifNull(attributes['WT_dcsdat'],'') AS cli_time,
attributes['WT_last_url'] AS last_url,
decodeURLComponent(attributes['WT_last_pagename']) AS last_pagename,
attributes['WT_current_url'] AS current_url,
decodeURLComponent(attributes['WT_current_pagename']) AS current_pagename,
decodeURLComponent(attributes['WT_module_name']) AS module_name,
attributes['WT_next_pageid'] AS next_pageid,
decodeURLComponent(attributes['WT_next_url']) AS next_url,
decodeURLComponent(attributes['WT_next_pagename']) AS next_pagename,
attributes['WT_component_id'] AS component_id,
decodeURLComponent(attributes['WT_input_mobile']) as input_mobile,
attributes['WT_touchcode'] AS touchcode,
attributes['WT_tpl'] AS tpl,
decodeURLComponent(attributes['WT_branch']) as branch,
decodeURLComponent(attributes['WT_es']) as complete_url,
attributes['WT_point_name'] AS point_name,
attributes['WT_component_name'] AS component_name,
attributes['WT_environment'] AS environment,
decodeURLComponent(attributes['WT_plat']) AS plat,
attributes['WT_sku_id'] AS sku_id,
decodeURLComponent(attributes['WT_prepare1']) as prepare1,
decodeURLComponent(attributes['WT_event_type']) AS event_type,
decodeURLComponent(attributes['WT_envName']) AS envname,
decodeURLComponent(attributes['WT_errCode']) AS errcode,
decodeURLComponent(attributes['WT_errMsg']) AS errmsg,
decodeURLComponent(attributes['WT_prov']) AS prov,
decodeURLComponent(attributes['WT_city']) AS city,
decodeURLComponent(attributes['WT_mc_ev_name']) AS mc_ev_name,
case when (data_source_id  ='dcscx966fo4l7j258ag0s874n_3w7x')then 'H5' 
         when data_source_id ='dcs47s4etp4l7jyla4pkwq3ox_5m9b' or data_source_id='dcsg4yobzk4bgdw3x3i02ujp5_7e9m' then '小程序'
         when data_source_id ='dcschlc2kl4bgdodrniom31n2_5i5n' then '公众号'
         else splitByString('_',ifNull(data_source_id,''))[2] end 
         as trmnl_style,
attributes['WT_tbd'] AS tbd,
ifNull(attributes['WT_dcsdat'],'') AS dcsdat,
decodeURLComponent(ifNull(user_agent,'')) as  user_agent,
attributes['WT_goods_id'] AS goods_id,
attributes['WT_sr'] AS sr,
attributes['WT_appId'] AS appid,
decodeURLComponent(attributes['WT_cid']) as cid,
decodeURLComponent(attributes['WT_userBrand']) as userbrand,
decodeURLComponent(attributes['WT_loginProvince']) as loginprovince,
decodeURLComponent(attributes['WT_loginCity']) as logincity,
attributes['WT_nodeId'] as nodeid,
attributes['WT_tv'] as tv,
attributes['WT_vt_f_tlh'] as vt_f_tlh,
attributes['WT_vtvs'] as vtvs,
'' as mark33,
decodeURLComponent(attributes['dcsuri']) as dcsuri,
decodeURLComponent(coalesce(attributes['WT_ti'],attributes['$title'])) as wt_ti,
attributes['WT_et'] as et,
attributes['WT_markId'] as markid,
attributes['WT_serial_no'] as serial_no,
attributes['WT_act_str_step_id'] as act_str_step_id,
attributes['WT_group_id'] as group_id,
attributes['WT_mr_id'] as mr_id,
attributes['WT_ad_step'] as ad_step,
attributes['WT_touch_tm'] as touch_tm,
attributes['WT_ed'] as ed,
decodeURLComponent(attributes['WT_XY']) as xy,
decodeURLComponent(attributes['WT_vt_sid']) as vt_sid, 
decodeURLComponent(attributes['WT_es']) as es,
decodeURLComponent(attributes['WT_sellerid']) AS wtsellerid,
decodeURLComponent(splitByString('%',cast(ifNull(attributes['WT_channelid'],'') as String))[1]) as wtchannelid,
decodeURLComponent(attributes['WT_pageid'])  as wtpageid,
event_id,
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
toDate(event_time) as dt,
if(length(cast(toHour(toDateTime(event_time)) as String))=1,concat('0',cast(toHour(toDateTime(event_time)) as String)),cast(toHour(toDateTime(event_time)) as String)) as hour,
data_source_id as dcsid
FROM olap.event_all
WHERE dt = '2023-06-28'
and hour = '10'
and data_source_id in ('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w','dcs49gd2jh65d2sj1tsa4rky3fd_7c2d','dcso02mhurci7zvoq6fa18exaf3_3s3i','dcsd38idvz3od4zmyowg1i6d8gx_7p1j','dcsfg7ph7uwat468njtzviizrfg_3a5k','dcs5pdfyfipmbxlfqmmciqw0a8x_8z2s','dcsz3akh3sibqou38s7guavsnr6_1w7q','dcs2skqkfpz7rtvxw6nzu4x4fgp_9w6z','dcsm3es4kqm3wtyo8gpi38qbf0j_8f4q','dcshtucfdltngkqtwvqniz0539u_2i1b','dcs8eooqrfuomftmsq6ewftia1h_6g7b','dcsxqbkupoawb04gcoq57azgdhx_0d4d','dcs6rxy80wuski0lw5qow0zt71c_8d2o','dcss2i3y89j99qo5kxpig0189i2_8m4a','dcs5pf9f1kksnaz4isjpsd7vnjo_5d3k','dcsl3qjy9cra6i10l5nz2rofjhe_3z9h','dcsc76toh4ik0az8db4ukqm84wd_4w6p','dcso43tjakd8vse5zksyuqz507q_5f8e','dcsm7pei65pvjao39ca1ams4nua_3j1d','dcsrsqxecu3ba3yqnifvv0ab9lm_9o0p','dcsppwpcwvuv2e82jv4dhm33v1c_0o1j','dcsjmisz40f127hqfku9tk6ds3n_3z7i','dcsdi06vzmk2pu9t8trixm63ctu_9b5g','dcsstwp3mj9nspzee8rta36eb9o_3r8r','dcsew4pj2u8stb71grgeavoev1w_3d2m','dcs4kes6i4udrbq7nbs1ctvql81_9m2x','dcs717bz69ty9p6a27c37oxohhf_8o8q','dcszcx0kbabmtltjrvlc1770m5q_2v6r','dcsco4d0qquihf4htc4ibo7v0ht_8j5v','dcsgl7axfbazclxywci71r3zqo9_4z6i','dcsh94u50ijasmndx4cym89ffsm_9y7c','dcstwoyw0krzaon00hpbx9hw5au_8s0b','dcs5hjggzsake53s886udkjdyei_1g3e','dcsxx79x7scb3ch36d1admiccz8_3n9u','dcsykjbxwjtpr16z9ky13i6yf2c_6c9w')
)tb1
left join (
select
tb3.anonymous_user as anonymous_user,
mobile
from (
select
distinct anonymous_user,
if(match(user, '.*==$'),aes_function(user),user) as mobile,
row_number() over (partition by anonymous_user order by event_time) as ct
from olap.event_all
where toDate(event_time) = '2023-06-28'
and if(length(cast(toHour(toDateTime(event_time)) as String))=1,concat('0',cast(toHour(toDateTime(event_time)) as String)),cast(toHour(toDateTime(event_time)) as String)) = '10'
and data_source_id in ('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w','dcs49gd2jh65d2sj1tsa4rky3fd_7c2d','dcso02mhurci7zvoq6fa18exaf3_3s3i','dcsd38idvz3od4zmyowg1i6d8gx_7p1j','dcsfg7ph7uwat468njtzviizrfg_3a5k','dcs5pdfyfipmbxlfqmmciqw0a8x_8z2s','dcsz3akh3sibqou38s7guavsnr6_1w7q','dcs2skqkfpz7rtvxw6nzu4x4fgp_9w6z','dcsm3es4kqm3wtyo8gpi38qbf0j_8f4q','dcshtucfdltngkqtwvqniz0539u_2i1b','dcs8eooqrfuomftmsq6ewftia1h_6g7b','dcsxqbkupoawb04gcoq57azgdhx_0d4d','dcs6rxy80wuski0lw5qow0zt71c_8d2o','dcss2i3y89j99qo5kxpig0189i2_8m4a','dcs5pf9f1kksnaz4isjpsd7vnjo_5d3k','dcsl3qjy9cra6i10l5nz2rofjhe_3z9h','dcsc76toh4ik0az8db4ukqm84wd_4w6p','dcso43tjakd8vse5zksyuqz507q_5f8e','dcsm7pei65pvjao39ca1ams4nua_3j1d','dcsrsqxecu3ba3yqnifvv0ab9lm_9o0p','dcsppwpcwvuv2e82jv4dhm33v1c_0o1j','dcsjmisz40f127hqfku9tk6ds3n_3z7i','dcsdi06vzmk2pu9t8trixm63ctu_9b5g','dcsstwp3mj9nspzee8rta36eb9o_3r8r','dcsew4pj2u8stb71grgeavoev1w_3d2m','dcs4kes6i4udrbq7nbs1ctvql81_9m2x','dcs717bz69ty9p6a27c37oxohhf_8o8q','dcszcx0kbabmtltjrvlc1770m5q_2v6r','dcsco4d0qquihf4htc4ibo7v0ht_8j5v','dcsgl7axfbazclxywci71r3zqo9_4z6i','dcsh94u50ijasmndx4cym89ffsm_9y7c','dcstwoyw0krzaon00hpbx9hw5au_8s0b','dcs5hjggzsake53s886udkjdyei_1g3e','dcsxx79x7scb3ch36d1admiccz8_3n9u','dcsykjbxwjtpr16z9ky13i6yf2c_6c9w')
and (match(user, '^[0-9]{11}$') or match(user, '.*==$'))
)tb3
where ct = 1
) tb2
on tb1.anonymous_user = tb2.anonymous_user
;