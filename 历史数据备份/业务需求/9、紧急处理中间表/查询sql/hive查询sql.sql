select 
next_pageid,next_pagename,touchcode,point_name,component_name,serial_no,act_str_step_id,group_id,mr_id,ad_step,touch_tm,ed,vt_sid
from ham.dwd_dcslog_event_di where dt = '2023-04-25'
and
(next_pageid is not null
or next_pagename is not null
or touchcode is not null
or point_name is not null
or component_name is not null
or serial_no is not null
or act_str_step_id is not null
or group_id is not null
or mr_id is not null
--or ad_step is not null
or touch_tm is not null
or ed is not null
or vt_sid is not null)
limit 10;