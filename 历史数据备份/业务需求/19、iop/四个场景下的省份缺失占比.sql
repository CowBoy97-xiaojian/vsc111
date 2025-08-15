1、登录事件，咱们这边无法准确判断登录，只能通过是否存在手机号来获取.
2、loginprovince为空问题，咨询过张静和夏老师后，确认的确存在为空情况，由于新规范下发不久，存量点位并没有完全改造。

 select advertype,wt_event,mark_id,count(wt_mobile),count(loginprovince)
  from ham_jituan.dwd_client_event_di tb1
  inner join 
  (
    select * from ham_jituan.dim_client_work_order_d 
    where dt='2023-07-05' 
    and area_name='业务楼层区域'
    and (instr(area_location,'权益专区')>0 or instr(area_location,'流量专区')>0 or instr(area_location,'套餐专区')>0 or instr(area_location,'终端专区')>0)
  )tb2
  on tb1.wt_event=tb2.marketing_id
  where tb1.dt='2023-07-05'
  group by advertype,wt_event,mark_id


  select dt,count(1) as pv,count(loginprovince) as prov_pv,count(logincity) as city_pv from dwd_client_event_di where dt between '2023-07-01' and '2023-07-04' and wt_mobile is not null group by dt order by dt;