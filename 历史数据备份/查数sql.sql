----------51007 ch_ods - dwd----------
在ch数据库执行如下sql:
select 'ods-dwd-51007中的ods',toDate(event_time) as dt,LPAD(toString(toHour(event_time)), 2, '0') as hour,count(1) as cnt
from olap.event_all
where dt='2023-12-01'
and event_type='custom_event'
and data_source_id in ('ac36424553dcd3e8','b95440ef47ec01fc','90be4403373b6463','86596eaccd0d746a','8aeb9b26885f3d8b','bdf908bd8e07b82c','938892fec03694af','b00057b79cbf85af','92f9b8b42859ed1c','94bbabc3a9686c5a','ad45b0b4c1ef7446','a9ae56608c62f805','ad3f51110dccb587','ad7c40e8ac8a0983','826f8be0db16938b','9328255238347f80','a930f2f2aee66a7c','b1b4618c1d4fac12','bee33e74dc5e9b38','aa4dbfdc0e193192','af82bebd8421abec','be5412a41f02e47a','8dd990e550265ae5','9c51cb5ab2e5d077','a20bffba73210972','9ead359aaf617556','98e2f7b831f876dd','aebed7d26ca2d38a','ab6b0c4315fa502b','9ed39aa37260081e','806586170173099d','ac34c865ecb163fa','a47b49395334c862','9e488aa27d948855','a1f48d9ff4f42571','b508a809cbbddd0b')
and attributes['area_id'] is null 
and attributes['type'] is null
group by dt,hour
order by dt,hour
format CSV
;

在hive数据库执行如下sql:
select dt,hour,count(1) as cnt
from ham_jituan.dwd_client_event_gio_di
where dt = '2023-12-01'
group by dt,hour
order by dt,hour
;

----------51006 ch_ods - dwd----------
在ch数据库执行如下sql:
select 'ods-dwd-51006中的ods',toDate(event_time) as dt,LPAD(toString(toHour(event_time)), 2, '0') as hour,count(1) as cnt
from olap.event_all
where dt='2023-12-01'
and event_type='custom_event'
and data_source_id in ('9249b85f328e4e9e','9a58d221e7eadf87','8177b467fba9fc0b','8a568ccb742bdc43','aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2','ac81a55cdf7074d5','999264cd2e773538','beb5471c243b2971','b5b9ccb1f69a35b9','a1f48d9ff4f42571','b508a809cbbddd0b','aae6afbf8823b1c1','a797613d647af6c6','a7464be8b200fe24','b9e74b9d5da046ce','b87eb8cec01051a6','b098587a3af15a25','b23efeab1461679b','a10b1f67c7d288c1','ae2308d5a08258b6','a04f0ad9925c080f','a6ba03d6884ee1c1','aa3a7b170aa41eed','a441e7427a44d390','adae9b8274a853f1','ad2e89b1010aed1e','bfe72e945deea64f','81ebc780373ee7cd','8d5d4407d1c3f6bf','abd580f08cf381e0','ba81a7647a8bed5d','8c19b47f237e28d3','a609de73d1690c0e','9c294cf845907ac1','b0839305cf5d280d','9a238b92b6612604','afdbd0f6b9a7ba30','8273883304ad70fb','86fdf7160c5cc8cc','81fab8d01c17ee89','89e7161d4a6b817f','9b50668a016469ec','bda85eeb52fb8b87','a8c29f7abfd639b4','b8d9fd437de471e4','875444ff6e049f33','853d176d98834573')
group by dt,hour
order by dt,hour
format CSV
;

在hive数据库执行如下sql:
select dt,hour,count(1) as cnt
from ham.dwd_dcslog_event_gio_di
where dt='2023-12-01'
group by dt,hour
order by dt,hour
;
----------51006----------
----------ads-dwd-要求数据量一致
----------有误重跑对应的账期: rpt_51006_gio_hourly
--ads-51006
--在hive数据库执行如下sql:

select dt, hour, count(1) as cnt
from ads_rpt_hachi_51006_gio_hourly
where dt='2023-12-01'
group by dt,hour
order by dt,hour;


--dwd
select dt, hour, sum(cnt) as cnt
from (
select dt, hour, count(1) as cnt
from ham.dwd_dcslog_event_gio_di tb1 
left join ham.dim_prov_code tb2 
on tb1.ip_prov=tb2.ip_prov 
left join ham.dim_final_city tb3 
on tb1.ip_city=tb3.city_name
where dt = '2023-12-01'
and trmnl_style in ('8a568ccb742bdc43','ac81a55cdf7074d5','aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2','999264cd2e773538','beb5471c243b2971','b5b9ccb1f69a35b9')
group by dt,hour
order by dt,hour

union all

select dt, hour, count(1) as cnt
from ham.dwd_dcslog_event_gio_di tb1 
inner join (
select distinct
domain,
interface
from ham.dim_domain_interface_di
where interface = '51006') tb4 
on tb1.domain=tb4.domain
where dt = '2023-12-01'
and trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b')
group by dt,hour
order by dt,hour
) tb1
group by dt,hour
order by dt,hour
;

----------51010----------

----------ads-dwd-要求数据量一致
--ads
select dt, hour, count(1) as cnt
from ham.ads_rpt_hachi_51010_hourly
where dt='2023-12-01'
group by dt,hour
order by dt,hour;

--dwd
select dt,hour,sum(cnt) as cnt
from (
select dt,hour,count(1) as cnt
from ham.dwd_dcslog_event_gio_di tb1 
left join ham.dim_prov_code tb2 
on tb1.ip_prov=tb2.ip_prov 
left join ham.dim_final_city tb3 
on tb1.ip_city=tb3.city_name
where dt = '2023-12-01'
and trmnl_style in ('9249b85f328e4e9e','9a58d221e7eadf87','8177b467fba9fc0b','aae6afbf8823b1c1','a797613d647af6c6','a7464be8b200fe24','b9e74b9d5da046ce','b87eb8cec01051a6','b098587a3af15a25','b23efeab1461679b','a10b1f67c7d288c1','ae2308d5a08258b6','a04f0ad9925c080f','a6ba03d6884ee1c1','aa3a7b170aa41eed','a441e7427a44d390','adae9b8274a853f1','ad2e89b1010aed1e','bfe72e945deea64f','81ebc780373ee7cd','8d5d4407d1c3f6bf','abd580f08cf381e0','ba81a7647a8bed5d','8c19b47f237e28d3','a609de73d1690c0e','9c294cf845907ac1','b0839305cf5d280d','9a238b92b6612604','afdbd0f6b9a7ba30','8273883304ad70fb','86fdf7160c5cc8cc','81fab8d01c17ee89','89e7161d4a6b817f','9b50668a016469ec','bda85eeb52fb8b87','a8c29f7abfd639b4','b8d9fd437de471e4','875444ff6e049f33','853d176d98834573')
group by dt,hour
order by dt,hour
union all
select dt,hour, count(1) as cnt
from ham.dwd_dcslog_event_gio_di tb1 
inner join (
select  distinct
domain,
interface
from ham.dim_domain_interface_di
where interface = '51010') tb4 
on tb1.domain=tb4.domain
where dt = '2023-12-01'
and trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b')
group by dt,hour
order by dt,hour
union all
select dt,hour, count(1) as cnt
FROM ham.ads_rpt_hachi_51006_dt_hour
WHERE dt = '2023-12-01'
and trmnl_style in ('7c2d','3s3i','7p1j','3a5k','8z2s','1w7q','9w6z','8f4q','2i1b','6g7b','0d4d','8d2o','8m4a','5d3k','3z9h','4w6p','5f8e','3j1d','9o0p','0o1j','3z7i','9b5g','3r8r','3d2m','9m2x','8o8q','2v6r','8j5v','4z6i','9y7c','8s0b','1g3e','3n9u','6c9w','2n3t')
group by dt,hour
order by dt,hour
) tb1
group by dt,hour
order by dt,hour
;

----------51007----------
--gio渠道ads-dwd对比,要求数据一致,有误重跑rpt51007gio
--ads-gio
select dt,hour,count(1) as cnt
from ham_jituan.ads_hachi_jzyy_xtb2_51007_gio_dt_hour
where dt='2023-12-01'
group by dt,hour
order by dt,hour
;
--dwd-gio
select dt,hour,sum(cnt) as cnt
from
(select dt,hour,count(1) as cnt
from ham_jituan.dwd_client_event_gio_di 
where dt='2023-12-01'
and (
    trmnl_style in ('b95440ef47ec01fc','90be4403373b6463')
or
    (trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b')
    and wt_event in ('c_perf','FlowDataCollection')
    )
)
group by dt,hour
order by dt,hour
union all
select dt,hour,count(1) as cnt
from ham_jituan.dwd_client_event_gio_di tb2
inner join (
select  distinct
domain,
interface
from ham.dim_domain_interface_di
where interface = '51007') tb4
on tb2.domain=tb4.domain
where dt='2023-12-01'
and trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b')
group by dt,hour
order by dt,hour
) tb1
group by dt,hour
order by dt,hour
;




select dt,count(1) 
from evevent_hi_client_imp_all 
where dt in ('2023-11-28','2023-11-29','2023-11-30') 
group by dt order by dt;

插码系统重点保障: 2023-12-02
整体运行正常    
一、系统巡检情况:
(1)软负载:当前连接并发量V20.8万/秒,V6:22.2万/秒;平均CPU、内存、磁盘使用率:7.9%、3.0%、65%。
(2)采集:
    collector:已无堆积;平均CPU、内存、磁盘使用率:34%、19%、53%
    shotpot:临时文件增量正常、切割正常,平均CPU、内存、磁盘使用率:56%、7%、72%。
(3)入库:生产速率3.48M,消费速率4.66M,积压388M。
(4)CDH集群:任务调度正常,HDFS/YARN服务正常。
(5)CH集群:平均CPU、内存、磁盘使用率:56%、22%、86%。
二、接口巡检情况:
20231201账期:
(1)51007: 1-9点调度完成,10-22点正在运行;
(2)51006: 1-9点调度完成,10-22点正在运行;
(3)51010: 1-9点调度完成,10-22点正在运行;



1、collector积压已经降下来了,最高堆积11k。
2、flink消费速率已经赶超生产速率,数据堆积正在下降。
3、集群一切正常。


select toDate(event_time) as dt,LPAD(toString(toHour(event_time)), 2, '0') as hour, count(1)
from olap.event_all
where dt = '2023-12-03'
group by dt,hour
order by dt,hour
FORMAT CSVWithNames;