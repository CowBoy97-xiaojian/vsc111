--有问题case when (wt_event like 'qwbn2110150001_%' or wt_event like 'qwbn2110150002_%') then splitByChar('_',wt_event)[0]  else wt_event end as wt_event
--有数据 splitBychar已经修改
-- 6、自定义原生部分ham.ads_hachi_jzyy_xtb2_51008
insert into table ham.ads_hachi_jzyy_xtb2_51008_all 
select wt_mobile          as bill_no
      ,replaceRegexpAll(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as imei
      ,wt_co_f            as cookie
      ,wt_prov            as prov_id
      ,ifnull(t4.name,'未知') as prov_name
      ,wt_city            as city_name
      ,channelid          as channel_id
      ,c_ip               as c_ip
      ,cs_host            as cs_host
      ,trmnl_style        as trmnl_style
      ,replaceRegexpAll(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')             as app_version
      ,wt_av              as sys_version
      ,date_day           as create_date
      ,date_time          as create_datetime
      ,click_time         as click_datetime
      ,'--'               as serial_id
      ,'--'               as start_time
      ,'--'               as down_time
      ,'--'               as status
      ,'--'               as issue_range
      ,'--'               as if_own_develop
      ,t2.frequency
      ,area_name
      ,t2.area_location
      ,'--'               as link_referer
      ,'--'               as link_current
      ,'--'               as url
      ,'--'               as issue_id
      ,mark_id            as mark_id
      ,marke_name         as mark_name
      ,'--'               as mark_type
      ,advertype          as advertype
      ,wt_event
      ,replaceRegexpAll(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')              as wt_ti
      ,wt_mc_id           as wt_mc_id
      ,wt_ac              as wt_ac
      ,t1.wt_ac_id        as wt_ac_id
      ,goods_id           as goods_id
      ,sku_id             as sku_id
      ,'--'               as editor_prov_name
      ,'--'               as mark_mode
      ,'--'               as if_prov_page
      ,'--'               as icon_code
      ,'--'               as login_type
      ,'--'               as is_webtends
      ,'--'               as if_safe_audit
      ,'--'               as editor
      ,'--'               as editor_phone_num
      ,'--'               as pro_company
      ,'--'               as template_id
      ,'自定义原生部分'        as analy_type
      ,conf_chan          as conf_chan
   ,point_type         as point_type
   ,area_type
   ,plat	as palt
,si_x	as si_x
,si_n	as si_n
,si_s	as si_s
,wt_goods_id	as wt_goods_id
,wt_sku_id	as wt_sku_id
,prepare1	as prepare1
,pageid
,sellerid
,extendid
,wt_channel_id
,wt_page_id
,wt_seller_id
,wt_extend_id
,dt 
,hour
from(select date_time   
           ,c_ip        
           ,cs_host     
           ,wt_cid      
           ,wt_co_f     
           ,wt_city     
           ,wt_mobile   
	   ,case when (ifnull(wt_event,'') like 'qwbn2110150001_%' or ifnull(wt_event,'') like 'qwbn2110150002_%') then splitByChar('_',ifnull(wt_event,''))[1]  else ifnull(wt_event,'') end as wt_event    
           ,wt_ti       
           ,wt_mc_id    
           ,wt_ac       
           ,wt_ac_id    
           ,goods_id    
           ,sku_id      
           ,cs_referer  
           ,wt_es       
           ,advertype   
           ,wt_areaname 
           ,mark_id     
           ,trmnl_style 
           ,wt_aav      
           ,wt_av       
           ,date_day    
           ,prov_name   
           ,click_time  
           ,wt_prov     
           ,channelid   
           ,dt 
           ,hour
,plat
,si_x
,si_n
,si_s
,wt_goods_id
,wt_sku_id
,prepare1
,pageid
,sellerid
,extendid
,wt_channel_id
,wt_page_id
,wt_seller_id
,wt_extend_id
     from webtrends.event_hi_client_all
     where dt= '2023-04-06'
     and trmnl_style in ('ANDROID','IOS','a1f48d9ff4f42571','b508a809cbbddd0b')) t1
inner join(select  '一级渠道'     as conf_chan
	       ,frequency     as frequency
	       ,'--'          as area_type
	       ,area_name     as area_name
	       ,marke_name    as area_location
	       ,marke_name    as marke_name
	       ,'普通全网配置类'          as marketing_mode
	       ,'未知'        as point_type
	       ,event
           from ham.dim_client_ld_zdy_d_all
where dt='2023-04-06'
) t2 
on t1.wt_event = t2.event  -- 自定义码表           
left join (select prov,name
           from ham.dim_client_city_all 
           group by prov,name) t4 
on t1.wt_prov = t4.prov
;