@牛仔很忙的； 左边是JZYY_CMBHPT_10002,右边是CM001实时
一级生产登录白皮书：https://shimo.im/docs/erAdPndVlKtZg2AG
wt---848018
select count(distinct mobile) 
from (
select
case when match(user, '^[0-9]{11}$') then user when match(user, '.*==$') and length(user)=24 then aes2_function(user) end AS mobile
from olap.event_all
where toDate(event_time) = '2023-08-06'
and attributes['WT_prov'] = '791'
and data_source_id in ('dcs47gbrugonmg3u1x8njabg1_2p4f','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcsgswzxehonmgrc8hz5w67g1_9o7q')
);

H5---742428
select count(distinct mobile) 
from (
select
case when match(user, '^[0-9]{11}$') then user when match(user, '.*==$') and length(user)=24 then aes2_function(user) end AS mobile
from olap.event_all
where toDate(event_time) = '2023-08-06'
and data_source_id in ('b95440ef47ec01fc')
and event_type = 'custom_event'
and attributes['WT_prov'] = '791'
)where mobile is not null;

原生的--7560280
select count(distinct mobile) 
from (
select
case when match(user, '^[0-9]{11}$') then user when match(user, '.*==$') and length(user)=24 then aes2_function(user) end AS mobile
from olap.event_all
where toDate(event_time) = '2023-08-06'
and data_source_id in ('a1f48d9ff4f42571','b508a809cbbddd0b')
and event_type = 'custom_event'
and attributes['WT_prov'] = '791'
and attributes['WT_event'] in ('FlowDataCollection','c_perf')
)where mobile is not null;



生产：965076


