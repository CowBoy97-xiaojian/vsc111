set hive.default.fileformat = Orc;
set hive.execution.engine = mr;
set hive.vectorized.execution.enabled  = false;

INSERT OVERWRITE TABLE ham.dwd_dcslog_event_di partition (dt = '${DT}', hour = '${HH}')
SELECT mobile,
       ck_id,
       ss_id,
       ip,
       area.prov,
       area.city,
       referer,
       event,
       split(es, '\\?')[0] AS url,
       si_n,
       si_s,
       si_x,
       nvl(nullif(split(channel_id,'%')[0], ''),nullif(substr(regexp_extract(es, 'channelId=([0-9a-zA-z]+)', 1),0,12),'')) AS channel_id,
       substr(nvl(nullif(page_id, ''), nullif(regexp_extract(es, 'pageId=([0-9a-zA-z]+)', 1),'')), 0, 19) AS page_id,
       input_xm,
       input_sfz,
       input_dz,
       mc_ev,
       nvl(nullif(seller_id, ''), nullif(regexp_extract(es, 'sellerId=([0-9a-zA-z]+)', 1),'')) AS seller_id,
       last_pageid, module_no, point_position, member, goods_no, login_status, 
       input_dq,
       input_buymobile,
       daytime,
       cli_time,
       dcsid,
       last_url,
       last_pagename,
       current_url,
       current_pagename,
       module_name,
       next_pageid,
       next_url,
       next_pagename,
       component_id,
       input_mobile,
       touchcode,
       tpl, 
       branch,
       es,
       point_name,
       component_name,
       environment,
       plat,
       sku_id,
       prepare1,
       event_type,
       envName,
       errCode,
       errMsg,
       prov,
       city,
       mc_ev_name,
       trmnl_style,
       tbd,
       dcsdat,
       user_agent,
       goods_id,
       sr,
       appid,
       cid,
       userbrand,
       loginprovince,
       logincity,
       nodeid,
       tv,
       vt_f_tlh,
       vtvs,
       '',--备用字段
       dcsuri,
       ti,
       et,
       markid,
       serial_no,
       act_str_step_id,
       group_id,
       mr_id,
       ad_step,
       touch_tm,
       ed,
       xy,
       vt_sid,
       es
FROM
  (SELECT coalesce(mobile, first_value(mobile, TRUE)
                   OVER (PARTITION BY ck_id
                         ORDER BY daytime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS mobile,
          ck_id,
          ss_id,
          ip,
          parse_ip(case when upper(nullif(ip,'')) = 'NULL' or ip is null or ip = 'wap.js.10086.cn' then '' else ip end) AS area,
          decode_url(referer) AS referer,
          query['WT.channelid'] AS channel_id,
          query['WT.pageid'] AS page_id,
          decode_url(query['WT.event']) AS event,
          decode_url(query['WT.es']) AS es,
          decode_url(query['WT.si_n']) AS si_n,
          decode_url(query['WT.si_s']) AS si_s,
          query['WT.si_x'] AS si_x,
          decode_url(decode_input(query['WT.input_xm'])) AS input_xm,
          decode_url(decode_input(query['WT.input_sfz'])) AS input_sfz,
          decode_url(decode_input(query['WT.input_dz'])) AS input_dz,
          query['WT.mc_ev'] AS mc_ev,
          query['WT.sellerid'] AS seller_id,
          query['WT.last_pageid'] AS last_pageid,
          query['WT.module_no'] AS module_no,
          decode_url(query['WT.point_position']) AS point_position,
          decode_url(query['WT.member']) AS member,
          query['WT.goods_no'] AS goods_no,
          query['WT.login_status'] AS login_status,
          query['WT.dcsdat'] AS cli_time,
          daytime,
          dcsid,
          query['WT.last_url'] AS last_url,
 	  decode_url(query['WT.last_pagename']) AS last_pagename,
	  query['WT.current_url'] AS current_url,
	  decode_url(query['WT.current_pagename']) AS current_pagename,
  	  decode_url(query['WT.module_name']) AS module_name,
	  query['WT.next_pageid'] AS next_pageid,
	  decode_url(query['WT.next_url']) AS next_url,
	  query['WT.next_pagename'] AS next_pagename,
	  query['WT.component_id'] AS component_id,
          decode_input(query['WT.input_mobile']) AS input_mobile,
          decode_input(query['WT.input_dq']) AS input_dq,
          decode_input(query['WT.input_buymobile']) AS input_buymobile,
	  query['WT.touchcode'] AS touchcode,
          query['WT.tpl'] AS tpl,
          query['WT.branch'] AS branch,
          query['WT.point_name'] AS point_name,
          query['WT.component_name'] AS component_name,
          query['WT.environment'] AS environment,
	  query['WT.plat'] AS plat,
          regexp_replace(query['WT.sku_id'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','_') AS sku_id,
          decode_url(query['WT.prepare1']) as prepare1,
          decode_url(query['WT.event_type']) AS event_type,
          decode_url(query['WT.envName']) AS envName,
          query['WT.errCode'] AS errCode,
          decode_url(query['WT.errMsg']) AS errMsg,
          query['WT.prov'] AS prov,
          query['WT.city'] AS city,
          decode_url(query['WT.mc_ev_name']) AS mc_ev_name,
          case      when dcsid ='3w7x' then 'H5' 
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
                    when dcsid ='7c2d' then '7c2d'
                    else null end 
          as trmnl_style,
  query['WT.tbd'] AS tbd,
  query['WT.dcsdat'] AS dcsdat,
  regexp_replace(user_agent,'\\|','_') as user_agent,
  regexp_replace(query['WT.goods_id'],'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','_') AS goods_id,
  query['WT.sr'] AS sr,
  query['WT.appId'] AS appid,
  query['WT.cid'] as cid,
  query['WT.userBrand'] as userbrand,
  query['WT.loginProvince'] as loginprovince,
  query['WT.loginCity'] as logincity,
  query['WT.nodeId'] as nodeid,
  query['WT.tv'] as tv,
  query['WT.vt_f_tlh'] as vt_f_tlh,
  query['WT.vtvs'] as vtvs,
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
  query['WT.vt_sid'] as vt_sid 
	   FROM ham.ods_dcslog_delta
   WHERE 1=1
     AND dt = '${DT}'
     AND hour = '${HH}') AS tba;
