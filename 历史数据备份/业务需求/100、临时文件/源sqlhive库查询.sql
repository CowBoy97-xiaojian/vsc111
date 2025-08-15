select prov_name,act_str_step_id,group_id,mr_id,touch_tm 
select carrieroperator
from ham_jituan.dwd_client_event_di
where dt = '2023-04-25'
and carrieroperator is not null limit 1;
(prov_name is not null
--or user_agent is not null
--or carrieroperator is not null
--or userbrand is not null
or act_str_step_id is not null
or group_id is not null
or mr_id is not null
or touch_tm is not null
)
limit 1;


user_agent,cs_host,carrieroperator,userbrand,wt_areaname

attributes['WT_carrierOperator']