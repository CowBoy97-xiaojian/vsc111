set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrick;

INSERT OVERWRITE TABLE ham.ads_rpt_hachi_40001 partition (code)
select daytime,
  ip,
  nullif(substr(regexp_extract(decode_url(query['WT.es']), 'channelId=([0-9a-zA-z]+)', 1),0,12),''),
  substr(nullif(regexp_extract(decode_url(query['WT.es']), 'pageId=([0-9a-zA-z]+)', 1),''), 0, 19),
  regexp_extract(decode_url(query['WT.es']), 'sellerId=([0-9-a-zA-z]+)',1),
  query['WT.dcsdat'] AS dcsdat,
  nullif(split(query['WT.channelid'],'%')[0], '') AS channelid,
  query['WT.co_f'] AS co_f,
  query['WT.event'] AS event,
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
  parse_ip(ip).prov,
  code 
from ods_dcslog_delta tb1 inner join dim_prov_code tb2 on parse_ip(ip).prov=ip_prov
where dt='${DT}' 
  and query['WT.mc_ev']='210315_QYCS'
  and daytime is not null;
