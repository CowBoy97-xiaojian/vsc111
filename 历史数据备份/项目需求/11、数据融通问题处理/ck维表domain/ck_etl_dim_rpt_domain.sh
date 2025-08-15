#!/bin/bash

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="truncate table ham.dim_rpt_hachi_domain_local on cluster cluster_gio_with_shard"

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="insert 
insert into ham.dim_rpt_hachi_domain_all
select
'51010',domain
from(
    select domain
    from olap.event_all
    where toDate(event_time) between date_sub(CAST('$1' AS date), 7) and CAST('$1' AS date)
    and data_source_id in ('aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2')  --51006渠道
    and match(attributes['WT_mc_ev'],'QYCS')=0
    group by domain
);"

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="
insert into ham.dim_rpt_hachi_domain_all(interface,domain)
select
'51010',domain
from(
    select domain
    from olap.event_all
    where toDate(event_time) between date_sub(CAST('$1' AS date), 7) and CAST('$1' AS date)
    and data_source_id in ('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w')  --51006渠道
    and match(attributes['WT_mc_ev'],'QYCS')=0
    group by domain
);"



insert into ham.dim_rpt_hachi_domain_all
select
'51010',domain
from(
    select domain
    from olap.event_all
    where toDate(event_time) between date_sub(CAST('2023-07-19' AS date), 7) and CAST('2023-07-19' AS date)
    and data_source_id in ('aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2')  --51006渠道
    and match(attributes['WT_mc_ev'],'QYCS')=0
    group by domain
);


select count(1)
from olap.event_all
where toDate(event_time) between '2023-07-13' and '2023-07-19'  --运行调度前一周
and data_source_id in ('aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2')  --51006渠道
and match(attributes['WT_mc_ev'],'QYCS') = 0;

('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w')

select
'51010',domain
from(
    select domain
    from olap.event_all
    where toDate(event_time) between date_sub(CAST('2023-07-19' AS date), 7) and CAST('2023-07-19' AS date)
    and data_source_id in ('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w')  --51006渠道
    and match(attributes['WT_mc_ev'],'QYCS')=0
    group by domain
);


clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query=" 
insert into ham.dim_rpt_hachi_domain_all(interface,domain)
select
'51010',tb.domain
from(
    select domain
    from olap.event_all
    where toDate(event_time) between date_sub(CAST('2023-07-19' AS date), 7) and CAST('2023-07-19' AS date)
    and data_source_id in ('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w')  --51006渠道
    and match(attributes['WT_mc_ev'],'QYCS')=0
    group by domain
) tb;"