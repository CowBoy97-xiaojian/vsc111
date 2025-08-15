select
last_pageid,module_no,point_position,member,goods_no,login_status,input_dq,input_buymobile,daytime,cli_time,last_url,last_pagename,current_url,current_pagename,module_name,next_pageid,next_url,next_pagename,component_id,input_mobile,touchcode,tpl,branch,complete_url,point_name,component_name,environment,plat,sku_id,prepare1,event_type,envname,errcode,errmsg,prov,city,mc_ev_name,trmnl_style,tbd,dcsdat,user_agent,goods_id,sr,appid,cid,userbrand,loginprovince,logincity,nodeid,tv,vt_f_tlh,vtvs,mark33,dcsuri,ti,et,markid,serial_no,act_str_step_id,group_id,mr_id,ad_step,touch_tm,ed,xy,vt_sid,es,wtsellerid,dt,hour,dcsid
from webtrends.event_hi_dcslog_all
where dt='2023-04-25' and hour = '09' 
and (
--last_pageid is not null
--or module_no is not null
--or point_position is not null
--or member is not null
goods_no is not null
--or login_status is not null
--or input_dq is not null
--or input_buymobile is not null
--or daytime is not null
--or cli_time is not null
--or last_url is not null
--or last_pagename is not null
--or current_url is not null
--or current_pagename is not null
--or module_name is not null
or next_pageid is not null
--or next_url is not null
or next_pagename is not null
--or component_id is not null
--or input_mobile is not null
or touchcode is not null
--or tpl is not null
--or branch is not null
--or complete_url is not null
or point_name is not null
or component_name is not null
--or environment is not null
--or plat is not null
--or sku_id is not null
--or prepare1 is not null
--or event_type is not null
--or envname is not null
--or errcode is not null
--or errmsg is not null
--or prov is not null
--or city is not null
--or mc_ev_name is not null
--or trmnl_style is not null
--or tbd is not null
--or dcsdat is not null
--or user_agent is not null
--or goods_id is not null
--or sr is not null
--or appid is not null
--or cid is not null
--or userbrand is not null
--or loginprovince is not null
--or logincity is not null
--or nodeid is not null
--or tv is not null
--or vt_f_tlh is not null
--or vtvs is not null
--or mark33 is not null
--or dcsuri is not null
--or ti is not null
--or et is not null
--or markid is not null
or serial_no is not null
or act_str_step_id is not null
or group_id is not null
or mr_id is not null
or ad_step is not null
or touch_tm is not null
or ed is not null
--or xy is not null
or vt_sid is not null
--or es is not null
--or wtsellerid is not null
)
limit 20 FORMAT CSVWithNames;