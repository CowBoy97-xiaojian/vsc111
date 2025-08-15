select MOBILE,WT_EVENT from (
SELECT
    MOBILE
    ,case when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_YMFWS'                 then 1   -- 初访用户
         when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_DJHMSRJPQDAN'       then 2   -- 兴趣用户
         when WT_EVENT = '220124_SCALSAASB_JHY_BLZYBZ1_DJBZTCQDAN'      then 3   -- 弹窗效果
         when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_DJQBLAN'            then 4   -- 意向用户
         when WT_EVENT = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW1GXX'      then 5   -- 意向办理搭售1业务用户
         when WT_EVENT = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW2GXX'      then 6   -- 意向办理搭售2业务用户
         when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_DJYZMSRJPQDAN'      then 7   -- 办理用户
         when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_DJQRBLAN'           then 8   -- 待支付用户
         when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_DJHGHMAN'           then 9   -- 校验不通过且还需办理用户
         else 0 end as WT_EVENT
from csap.TB_DWD_CT_OLSP_DETAILWEBPAGE_HOUR_51006
where STATIS_DATE between '20230523' and '20230531')
where WT_EVENT != 0 group by MOBILE,WT_EVENT ;


select mobile,wt_event 
from 
(select mobile,dt,
case when event = '220124_SCALSAASB_JHY_BLZY_YMFWS' then 1
when event = '220124_SCALSAASB_JHY_BLZY_DJHMSRJPQDAN' then 2
when event = '220124_SCALSAASB_JHY_BLZYBZ1_DJBZTCQDAN' then 3
when event = '220124_SCALSAASB_JHY_BLZY_DJQBLAN' then 4
when event = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW1GXX' then 5
when event = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW2GXX' then 6
when event = '220124_SCALSAASB_JHY_BLZY_DJYZMSRJPQDAN' then 7
when event = '220124_SCALSAASB_JHY_BLZY_DJQRBLAN' then 8
when event = '220124_SCALSAASB_JHY_BLZY_DJHGHMAN' then 9
else 0 end as wt_event
from ham.dwd_dcslog_event_di
where 
--dt between '2023-05-23' and '2023-05-31'
dt = '2023-05-25'
) t1
where wt_event != 0 
group by mobile,wt_event;


select mobile,wt_event 
from 
(select mobile,dt,
case when wtevent = '220124_SCALSAASB_JHY_BLZY_YMFWS' then 1
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJHMSRJPQDAN' then 2
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJBZTCQDAN' then 3
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJQBLAN' then 4
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW1GXX' then 5
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW2GXX' then 6
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJYZMSRJPQDAN' then 7
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJQRBLAN' then 8
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJHGHMAN' then 9
else 0 end as wt_event
from ham.ads_rpt_hachi_51006_dt_hour
where 
--dt between '2023-05-23' and '2023-05-31'
dt = '2023-05-2'
) t1
where wt_event != 0 
group by mobile,wt_event;

1190

select t_dt,count(1) 
from (
select t1.dt as t_dt,mobile,wt_event 
from 
(select mobile,dt,
case when wtevent = '220124_SCALSAASB_JHY_BLZY_YMFWS' then 1
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJHMSRJPQDAN' then 2
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJBZTCQDAN' then 3
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJQBLAN' then 4
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW1GXX' then 5
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW2GXX' then 6
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJYZMSRJPQDAN' then 7
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJQRBLAN' then 8
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJHGHMAN' then 9
else 0 end as wt_event
from ham.ads_rpt_hachi_51006_dt_hour
where 
dt between '2023-05-23' and '2023-05-26'
) t1
where wt_event != 0 
group by dt,mobile,wt_event
) t2
group by t_dt;

select count(1) from
(
select mobile from 
(select mobile,dt,
case when wtevent = '220124_SCALSAASB_JHY_BLZY_YMFWS' then 1
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJHMSRJPQDAN' then 2
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJBZTCQDAN' then 3
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJQBLAN' then 4
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW1GXX' then 5
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW2GXX' then 6
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJYZMSRJPQDAN' then 7
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJQRBLAN' then 8
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJHGHMAN' then 9
else 0 end as wt_event
from ham.ads_rpt_hachi_51006_dt_hour
where 
--dt between '2023-05-23' and '2023-05-31'
dt in ('2023-05-23','2023-05-24','2023-05-25','2023-05-26')
) t1
where wt_event != 0 
group by mobile,wt_event);


2023-06-05



select count(mobile) FROM
(
SELECT
 MOBILE
 ,case when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_YMFWS'        then 1   -- 初访用户
   when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_DJHMSRJPQDAN'    then 2   -- 兴趣用户
   when WT_EVENT = '220124_SCALSAASB_JHY_BLZYBZ1_DJBZTCQDAN'      then 3   -- 弹窗效果
   when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_DJQBLAN'        then 4   -- 意向用户
   when WT_EVENT = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW1GXX'      then 5   -- 意向办理搭售1业务用户
   when WT_EVENT = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW2GXX'     then 6   -- 意向办理搭售2业务用户
   when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_DJYZMSRJPQDAN'      then 7   -- 办理用户
   when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_DJQRBLAN'        then 8   -- 待支付用户
   when WT_EVENT = '220124_SCALSAASB_JHY_BLZY_DJHGHMAN'        then 9   -- 校验不通过且还需办理用户
   else 0 end as WT_EVENT
from csap.TB_DWD_CT_OLSP_DETAILWEBPAGE_HOUR_51006
where STATIS_DATE = '20230528'
  group by MOBILE,WT_EVENT)a
where WT_EVENT <> 0


修改后hive查数
select count(mobile) FROM
(
SELECT
mobile
,case when wtevent = '220124_SCALSAASB_JHY_BLZY_YMFWS' then 1 
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJHMSRJPQDAN' then 2 
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJBZTCQDAN' then 3 
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJQBLAN' then 4 
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW1GXX' then 5 
when wtevent = '220124_SCALSAASB_JHY_BLZYBZ1_DJDSYW2GXX'  then 6 
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJYZMSRJPQDAN' then 7 
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJQRBLAN' then 8 
when wtevent = '220124_SCALSAASB_JHY_BLZY_DJHGHMAN' then 9 
else 0 end as wtevent
from ham.ads_rpt_hachi_51006_dt_hour
where dt = '2023-05-28'
 group by mobile,wtevent) a
where wtevent != 0;