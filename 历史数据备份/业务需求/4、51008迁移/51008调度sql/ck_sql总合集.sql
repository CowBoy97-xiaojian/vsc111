
-- 1、有重复工单
drop table ptemp.dw_c_cth_cm_log_d_01;
create table ptemp.dw_c_cth_cm_log_d_01
as select serial_id           
,start_time          
,down_time           
,status              
,issue_range         
,if_own_develop      
,area_name           
,area_location       
,issue_id            
,marke_name          
,url                 
,editor_provname     
,a.frequency           
,marketing_mode      
,if_prov_page        
,a.marketing_id
,icon_code            
,login_type          
,is_webtends         
,if_safe_audit       
,editor              
,editor_phone_num    
,pro_company         
,template_id         
,marketing_type      
,wt_ac_id   
,conf_chan
,area_type
from(select serial_id           
,start_time          
,down_time           
,status              
,issue_range         
,if_own_develop      
,area_name       
,area_location 
,issue_id 
,marke_name 
,url 
,editor_provname
,frequency  
,marketing_mode
,if_prov_page
,case when match(marketing_id,'_') > 0 then splitByChar('_',marketing_id)[1] else marketing_id end as marketing_id  -- 20210916修改，增加历史数据兼容
,icon_code
,login_type
,is_webtends         
,if_safe_audit
,editor
,editor_phone_num
,pro_company
,template_id
,marketing_type
,wt_ac_id
,conf_chan
,area_type
from ham.dim_client_ord_info_d 
where dt='${DT}') a
inner join(select frequency,marketing_id,count(1) as pv 
from(select start_time, down_time, editor_provname, 
area_name, area_location, marke_name, url,
frequency, marketing_mode, 
case when match(marketing_id,'_') > 0 then splitByChar('_',marketing_id)[1] else marketing_id end as marketing_id  -- 20210916修改，增加历史数据兼容
,dt,count(1) as pv 
from ham.dim_client_ord_info_d 
where dt = '${DT}'
group by start_time, down_time, editor_provname, area_name, area_location, marke_name, url,frequency, marketing_mode, marketing_id, dt) a           
group by frequency
,marketing_id 
having pv > 1) b --判断重复频道，如果有 增加匹配条件
on a.frequency=b.frequency
and a.marketing_id=b.marketing_id
;


-- 2、无重复工单
drop table ptemp.dw_c_cth_cm_log_d_02;
create table ptemp.dw_c_cth_cm_log_d_02
as select  serial_id           
,start_time          
,down_time           
,status              
,issue_range         
,if_own_develop      
,area_name           
,area_location       
,issue_id            
,marke_name          
,url                 
,editor_provname     
,a.frequency           
,marketing_mode      
,if_prov_page        
,a.marketing_id
,icon_code            
,login_type          
,is_webtends         
,if_safe_audit       
,editor              
,editor_phone_num    
,pro_company         
,template_id         
,marketing_type      
,wt_ac_id   
,conf_chan
,area_type
from(select serial_id           
,start_time          
,down_time           
,status              
,issue_range         
,if_own_develop      
,area_name       
,area_location 
,issue_id 
,marke_name 
,url 
,editor_provname
,frequency  
,marketing_mode
,if_prov_page
,case when match(marketing_id,'_') > 0 then splitByChar('_',marketing_id)[1] else marketing_id end as marketing_id  -- 20210916修改，增加历史数据兼容
,icon_code
,login_type
,is_webtends         
,if_safe_audit
,editor
,editor_phone_num
,pro_company
,template_id
,marketing_type
,wt_ac_id
,conf_chan
,area_type
from ham.dim_client_ord_info_d 
where dt='${DT}') a
inner join(select frequency,marketing_id,count(1) as pv 
from(select start_time, down_time, editor_provname, 
area_name, area_location, marke_name, url,
frequency, marketing_mode, 
case when match(marketing_id,'_') > 0 then splitByChar('_',marketing_id)[1] else marketing_id end as marketing_id  -- 20210916修改，增加历史数据兼容
,dt,count(1) as pv 
from ham.dim_client_ord_info_d 
where dt = '${DT}'
group by start_time, down_time, editor_provname, area_name, area_location, marke_name, url,frequency, marketing_mode, marketing_id, dt) a           
group by frequency
,marketing_id 
having pv = 1) b --判断重复频道，如果有 增加匹配条件
on a.frequency=b.frequency
and a.marketing_id=b.marketing_id
;


-- 3、h5_工单重复部分
insert into table ham.ads_hachi_jzyy_xtb2_51008 
select        wt_mobile          as bill_no
,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')  as imei
,wt_co_f            as cookie
,wt_prov            as prov_id
,nvl(t4.name,'未知') as prov_name
,wt_city            as city_name
,channelid          as channel_id
,c_ip               as c_ip
,cs_host            as cs_host
,trmnl_style        as trmnl_style
,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')             as app_version
,wt_av              as sys_version
,date_day           as create_date
,date_time          as create_datetime
,click_time         as click_datetime
,serial_id          as serial_id
,start_time         as start_time
,down_time          as down_time
,status             as status
,issue_range        as issue_range
,if_own_develop     as if_own_develop
,frequency  
,area_name          as area_name
,case when (t2.frequency = '首页' or t2.frequency = '我的') and t2.area_name = 'icon功能区' then concat('icon(',area_location,')') 
else t2.area_location end as area_location
,cs_referer         as link_referer
,wt_es              as link_current
,case when nvl(t2.url,'')='' then '--' 
else t2.url end as url
,issue_id           as issue_id
,mark_id            as mark_id
,marke_name         as mark_name
,marketing_type     as mark_type
,advertype          as advertype
,case when wt_event like '2020bjjtzq_yxqy_%' then regexp_extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d',0)
when wt_event like 'sy_qy_%' then concat(regexp_extract(wt_event,'sy_qy_\\d+-\\d+',0),reverse(regexp_extract(reverse(wt_event),'\\d+-\\d+-\\d+',0))) 
else wt_event end as wt_event
,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')              as wt_ti
,wt_mc_id           as wt_mc_id
,wt_ac              as wt_ac
,nvl(t1.wt_ac_id,t2.wt_ac_id)    as wt_ac_id
,goods_id           as goods_id
,sku_id             as sku_id
,editor_provname    as editor_prov_name
,marketing_mode     as mark_mode
,if_prov_page       as if_prov_page
,icon_code          as icon_code
,login_type         as login_type
,is_webtends        as is_webtends
,if_safe_audit      as if_safe_audit
,editor             as editor
,editor_phone_num   as editor_phone_num
,pro_company        as pro_company
,template_id        as template_id
,'h5_工单重复部分'  as analy_type
,conf_chan
,point_type
,area_type
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
from(select date_time   
,c_ip        
,cs_host     
,wt_cid      
,wt_co_f     
,wt_city     
,wt_mobile   
,wt_event    
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
from ham.dwd_client_event_di
where dt= '${DT}'
and trmnl_style = 'h5') t1
inner join(select  serial_id           
,start_time          
,down_time           
,status              
,issue_range         
,if_own_develop      
,area_name           
,area_location       
,issue_id            
,marke_name          
,url                 
,editor_provname     
,frequency           
,marketing_mode      
,if_prov_page        
,marketing_id        
,icon_code            
,login_type          
,is_webtends         
,if_safe_audit       
,editor              
,editor_phone_num    
,pro_company         
,template_id         
,marketing_type      
,wt_ac_id   
,conf_chan
,case when (frequency = '首页' or frequency = '我的') and area_type = 'icon功能区' then 'icon区域'
when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'
else '其他' end as point_type
,area_type
from ptemp.dw_c_cth_cm_log_d_01) t2 --工单表
on t1.mark_id = t2.marketing_id  
inner join ham.dim_client_h5 t3 --码表
on t2.area_name = t3.quyu             
left join (select prov,name
from ham.dim_client_city 
group by prov,name) t4 
on t1.wt_prov = t4.prov                --地市翻译表                      
where locate(t3.event,t1.wt_event) > 0 
and t3.pindao = t2.frequency
;


-- 4、h5新版本数据
insert into table ham.ads_hachi_jzyy_xtb2_51008 
select        wt_mobile          as bill_no
,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as imei
,wt_co_f            as cookie
,wt_prov            as prov_id
,nvl(t4.name,'未知') as prov_name
,wt_city            as city_name
,channelid          as channel_id
,c_ip               as c_ip
,cs_host            as cs_host
,trmnl_style        as trmnl_style
,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')             as app_version
,wt_av              as sys_version
,date_day           as create_date
,date_time          as create_datetime
,click_time         as click_datetime
,serial_id          as serial_id
,start_time         as start_time
,down_time          as down_time
,status             as status
,issue_range        as issue_range
,if_own_develop     as if_own_develop
,frequency  
,area_name
,case when (t2.frequency = '首页' or t2.frequency = '我的') and t2.area_name = 'icon功能区' then concat('icon(',area_location,')') 
else t2.area_location end as area_location
,cs_referer         as link_referer
,wt_es              as link_current
,case when nvl(t2.url,'')='' then '--' 
else t2.url end as url
,issue_id           as issue_id
,mark_id            as mark_id
,marke_name         as mark_name
,marketing_type     as mark_type
,advertype          as advertype
,case when wt_event like '2020bjjtzq_yxqy_%' then regexp_extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d',0)
when wt_event like 'sy_qy_%' then concat(regexp_extract(wt_event,'sy_qy_\\d+-\\d+',0),reverse(regexp_extract(reverse(wt_event),'\\d+-\\d+-\\d+',0))) 
else wt_event end as wt_event
,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')              as wt_ti
,wt_mc_id           as wt_mc_id
,wt_ac              as wt_ac
,nvl(t1.wt_ac_id,t2.wt_ac_id)    as wt_ac_id
,goods_id           as goods_id
,sku_id             as sku_id
,editor_provname    as editor_prov_name
,marketing_mode     as mark_mode
,if_prov_page       as if_prov_page
,icon_code          as icon_code
,login_type         as login_type
,is_webtends        as is_webtends
,if_safe_audit      as if_safe_audit
,editor             as editor
,editor_phone_num   as editor_phone_num
,pro_company        as pro_company
,template_id        as template_id
,'h5新版本' as analy_type 
,conf_chan
,point_type
,area_type
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
from (select date_time   
,c_ip        
,cs_host     
,wt_cid      
,wt_co_f     
,wt_city     
,wt_mobile   
,wt_event    
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
from ham.dwd_client_event_di
where dt= '${DT}' ) as t1
inner join (select serial_id           
,start_time          
,down_time           
,status              
,issue_range         
,if_own_develop      
,area_name       
,area_location 
,issue_id 
,marke_name 
,url 
,editor_provname
,frequency  
,marketing_mode
,if_prov_page
,marketing_id
,icon_code
,login_type
,is_webtends         
,if_safe_audit
,editor
,editor_phone_num
,pro_company
,template_id
,marketing_type
,wt_ac_id
,conf_chan
,case when (frequency = '首页' or frequency = '我的') and area_type = 'icon功能区' then 'icon区域'
when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'
else '其他' end as point_type
,area_type 
from ham.dim_client_ord_info_d 
where dt='${DT}' )t2 on t1.wt_event = t2.marketing_id --工单表
left join (select prov,name from ham.dim_client_city  group by prov,name) t4 on t1.wt_prov = t4.prov
where trmnl_style = 'h5'
;


-- 5、原生
insert into table ham.ads_hachi_jzyy_xtb2_51008 
select        wt_mobile          as bill_no
,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as imei
,wt_co_f            as cookie
,wt_prov            as prov_id
,nvl(t4.name,'未知') as prov_name
,wt_city            as city_name
,channelid          as channel_id
,c_ip               as c_ip
,cs_host            as cs_host
,trmnl_style        as trmnl_style
,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')             as app_version
,wt_av              as sys_version
,date_day           as create_date
,date_time          as create_datetime
,click_time         as click_datetime
,serial_id          as serial_id
,start_time         as start_time
,down_time          as down_time
,status             as status
,issue_range        as issue_range
,if_own_develop     as if_own_develop
,frequency  
,area_name
,case when (t2.frequency = '首页' or t2.frequency = '我的') and t2.area_name = 'icon功能区' then concat('icon(',area_location,')') 
else t2.area_location end as area_location
,cs_referer         as link_referer
,wt_es              as link_current
,case when nvl(t2.url,'')='' then '--' 
else t2.url end as url
,issue_id           as issue_id
,mark_id            as mark_id
,marke_name         as mark_name
,marketing_type     as mark_type
,advertype          as advertype
,case when wt_event like '2020bjjtzq_yxqy_%' then regexp_extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d',0)
when wt_event like 'sy_qy_%' then concat(regexp_extract(wt_event,'sy_qy_\\d+-\\d+',0),reverse(regexp_extract(reverse(wt_event),'\\d+-\\d+-\\d+',0))) 
else wt_event end as wt_event
,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')              as wt_ti
,wt_mc_id           as wt_mc_id
,wt_ac              as wt_ac
,nvl(t1.wt_ac_id,t2.wt_ac_id)    as wt_ac_id
,goods_id           as goods_id
,sku_id             as sku_id
,editor_provname    as editor_prov_name
,marketing_mode     as mark_mode
,if_prov_page       as if_prov_page
,icon_code          as icon_code
,login_type         as login_type
,is_webtends        as is_webtends
,if_safe_audit      as if_safe_audit
,editor             as editor
,editor_phone_num   as editor_phone_num
,pro_company        as pro_company
,template_id        as template_id
,'原生_工单'  as analy_type
,conf_chan
,point_type
,area_type
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
from(select date_time   
,c_ip        
,cs_host     
,wt_cid      
,wt_co_f     
,wt_city     
,wt_mobile   
,wt_event    
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
from ham.dwd_client_event_di
where dt= '${DT}'
and trmnl_style in ('android','ios')) t1
inner join(select  serial_id           
,start_time          
,down_time           
,status              
,issue_range         
,if_own_develop      
,area_name           
,area_location       
,issue_id            
,marke_name          
,url                 
,editor_provname  
,frequency 
,marketing_mode      
,if_prov_page        
,marketing_id        
,icon_code            
,login_type          
,is_webtends         
,if_safe_audit       
,editor              
,editor_phone_num    
,pro_company         
,template_id         
,marketing_type      
,wt_ac_id   
,conf_chan
,case when (frequency = '首页' or frequency = '我的') and area_type = 'icon功能区' then 'icon区域'
when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'
else '其他' end as point_type
,area_type 
from ham.dim_client_ord_info_d where dt='${DT}' ) t2 --工单表
on  concat(t1.advertype,'_',t1.mark_id) = t2.marketing_id  
left join (select prov,name
from ham.dim_client_city 
group by prov,name) t4 
on t1.wt_prov = t4.prov                --地市翻译表
;


-- 自定义原生部分ham.ads_hachi_jzyy_xtb2_51008
insert into table ham.ads_hachi_jzyy_xtb2_51008 
select wt_mobile          as bill_no
      ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as imei
      ,wt_co_f            as cookie
      ,wt_prov            as prov_id
      ,nvl(t4.name,'未知') as prov_name
      ,wt_city            as city_name
      ,channelid          as channel_id
      ,c_ip               as c_ip
      ,cs_host            as cs_host
      ,trmnl_style        as trmnl_style
      ,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')             as app_version
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
      ,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')              as wt_ti
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
from(select date_time   
           ,c_ip        
           ,cs_host     
           ,wt_cid      
           ,wt_co_f     
           ,wt_city     
           ,wt_mobile   
	   ,case when (wt_event like 'qwbn2110150001_%' or wt_event like 'qwbn2110150002_%') then splitByChar('_',wt_event)[0]  else wt_event end as wt_event    
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
     from ham.dwd_client_event_di
     where dt= '${DT}'
     and trmnl_style in ('android','ios')) t1
inner join(select  '一级渠道'     as conf_chan
	       ,frequency     as frequency
	       ,'--'          as area_type
	       ,area_name     as area_name
	       ,marke_name    as area_location
	       ,marke_name    as marke_name
	       ,'普通全网配置类'          as marketing_mode
	       ,'未知'        as point_type
	       ,event
           from ham.dim_client_ld_zdy_d
where dt='${DT}'
) t2 
on t1.wt_event = t2.event  -- 自定义码表           
left join (select prov,name
           from ham.dim_client_city 
           group by prov,name) t4 
on t1.wt_prov = t4.prov
;


-- 7、h5_工单无重复部分ham.ads_hachi_jzyy_xtb2_51008
insert into table ham.ads_hachi_jzyy_xtb2_51008 
select        wt_mobile          as bill_no
             ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as imei
             ,wt_co_f            as cookie
             ,wt_prov            as prov_id
             ,nvl(t4.name,'未知') as prov_name
             ,wt_city            as city_name
             ,channelid          as channel_id
             ,c_ip               as c_ip
             ,cs_host            as cs_host
             ,trmnl_style        as trmnl_style
             ,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')             as app_version
             ,wt_av              as sys_version
             ,date_day           as create_date
             ,date_time          as create_datetime
             ,click_time         as click_datetime
             ,serial_id          as serial_id
             ,start_time         as start_time
             ,down_time          as down_time
             ,status             as status
             ,issue_range        as issue_range
             ,if_own_develop     as if_own_develop
             ,frequency 
             ,area_name          as area_name
             ,case when (t2.frequency = '首页' or t2.frequency = '我的') and t2.area_name = 'icon功能区' then concat('icon(',area_location,')') 
                   else t2.area_location end as area_location
             ,cs_referer         as link_referer
             ,wt_es              as link_current
             ,case when nvl(t2.url,'')='' then '--' 
                   else t2.url end as url
             ,issue_id           as issue_id
             ,mark_id            as mark_id
             ,marke_name         as mark_name
             ,marketing_type     as mark_type
             ,advertype          as advertype
             ,case when wt_event like '2020bjjtzq_yxqy_%' then regexp_extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d',0)
                   when wt_event like 'sy_qy_%' then concat(regexp_extract(wt_event,'sy_qy_\\d+-\\d+',0),reverse(regexp_extract(reverse(wt_event),'\\d+-\\d+-\\d+',0))) 
                   else wt_event end as wt_event
             ,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')              as wt_ti
             ,wt_mc_id           as wt_mc_id
             ,wt_ac              as wt_ac
             ,nvl(t1.wt_ac_id,t2.wt_ac_id)    as wt_ac_id
             ,goods_id           as goods_id
             ,sku_id             as sku_id
             ,editor_provname    as editor_prov_name
             ,marketing_mode     as mark_mode
             ,if_prov_page       as if_prov_page
             ,icon_code          as icon_code
             ,login_type         as login_type
             ,is_webtends        as is_webtends
             ,if_safe_audit      as if_safe_audit
             ,editor             as editor
             ,editor_phone_num   as editor_phone_num
             ,pro_company        as pro_company
             ,template_id        as template_id
             ,'h5_工单无重复'    as analy_type
             ,conf_chan
             ,point_type
             ,area_type
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
from(select date_time   
           ,c_ip        
           ,cs_host     
           ,wt_cid      
           ,wt_co_f     
           ,wt_city     
           ,wt_mobile   
           ,wt_event    
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
     from ham.dwd_client_event_di
     where dt= '${DT}'
     and trmnl_style = 'h5') t1
inner join(select  serial_id           
                  ,start_time          
                  ,down_time           
                  ,status              
                  ,issue_range         
                  ,if_own_develop      
                  ,area_name           
                  ,area_location       
                  ,issue_id            
                  ,marke_name          
                  ,url                 
                  ,editor_provname     
                  ,frequency           
                  ,marketing_mode      
                  ,if_prov_page        
                  ,marketing_id        
                  ,icon_code            
                  ,login_type          
                  ,is_webtends         
                  ,if_safe_audit       
                  ,editor              
                  ,editor_phone_num    
                  ,pro_company         
                  ,template_id         
                  ,marketing_type      
                  ,wt_ac_id   
               ,conf_chan
,case when (frequency = '首页' or frequency = '我的') and area_type = 'icon功能区' then 'icon区域'
when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'
else '其他' end as point_type
,area_type 
           from ptemp.dw_c_cth_cm_log_d_02) t2 --工单表
on t1.mark_id = t2.marketing_id  
inner join ham.dim_client_h5 t3 --码表
on 1 = 1            
left join (select prov,name
           from ham.dim_client_city 
           group by prov,name) t4 
on t1.wt_prov = t4.prov                --地市翻译表                      
where locate(t3.event,t1.wt_event) > 0 
and t3.pindao = t2.frequency
;

-- 8、自定义h5部分ham.ads_hachi_jzyy_xtb2_51008
insert into table ham.ads_hachi_jzyy_xtb2_51008 
select  wt_mobile         as bill_no
       ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as imei
       ,wt_co_f           as cookie
       ,wt_prov           as prov_id
       ,nvl(t4.name,'未知') as prov_name
       ,wt_city           as city_name
       ,channelid         as channel_id
       ,c_ip              as c_ip
       ,cs_host           as cs_host
       ,trmnl_style       as trmnl_style
       ,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')            as app_version
       ,wt_av             as sys_version
       ,date_day               as create_date
       ,date_time         as create_datetime
       ,click_time        as click_datetime
       ,'--'              as serial_id
       ,'--'              as start_time
       ,'--'              as down_time
       ,'--'              as status
       ,'--'              as issue_range
       ,'--'              as if_own_develop
       ,t2.frequency
       ,area_name
       ,t2.area_location
       ,'--'              as link_referer
       ,'--'              as link_current
       ,'--'              as url
       ,'--'              as issue_id
       ,mark_id           as mark_id
       ,marke_name        as mark_name
       ,'--'              as mark_type
       ,advertype         as advertype
       ,wt_event
       ,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')             as wt_ti
       ,wt_mc_id          as wt_mc_id
       ,wt_ac             as wt_ac
       ,t1.wt_ac_id       as wt_ac_id
       ,goods_id          as goods_id
       ,sku_id            as sku_id
       ,'--'              as editor_prov_name
       ,'--'              as mark_mode
       ,'--'              as if_prov_page
       ,'--'              as icon_code
       ,'--'              as login_type
       ,'--'              as is_webtends
       ,'--'              as if_safe_audit
       ,'--'              as editor
       ,'--'              as editor_phone_num
       ,'--'              as pro_company
       ,'--'              as template_id
       ,'自定义h5部分'           as analy_type
       ,conf_chan         as conf_chan
       ,point_type        as point_type
       ,area_type
       ,plat        as palt
,si_x   as si_x
,si_n   as si_n
,si_s   as si_s
,wt_goods_id    as wt_goods_id
,wt_sku_id      as wt_sku_id
,prepare1       as prepare1
,pageid
,sellerid
,extendid
,wt_channel_id
,wt_page_id
,wt_seller_id
,wt_extend_id
from
(
	select  date_time
	       ,c_ip
	       ,cs_host
	       ,wt_cid
	       ,wt_co_f
	       ,wt_city
	       ,wt_mobile
	       ,case when (wt_event like 'qwbn2110150001_%' or wt_event like 'qwbn2110150002_%') then splitByChar('_',wt_event)[0]  else wt_event end as wt_event
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
	from ham.dwd_client_event_di
	where dt= '${DT}'
	and trmnl_style in ('h5')
       and advertype = 122
) t1
inner join
(
	select  '一级渠道'     as conf_chan
	       ,frequency     as frequency
	       ,'--'          as area_type
	       ,area_name     as area_name
	       ,marke_name    as area_location
	       ,marke_name    as marke_name
	       ,'普通全网配置类'          as marketing_mode
	       ,'未知'        as point_type
	       ,event
              ,mark_id   as wt_mark_id
	from ham.dim_client_ld_zdy_d
       where dt='${DT}'
) t2
on t1.mark_id = t2.wt_mark_id -- 自定义事件码表日表
left join
(
	select  prov,name
	from ham.dim_client_city 
	group by prov,name
) t4
on t1.wt_prov = t4.prov -- 地市翻译表 
;


-- 9、宽带专区ham.ads_hachi_jzyy_xtb2_51008
insert into table ham.ads_hachi_jzyy_xtb2_51008 
select        wt_mobile          as bill_no
             ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as imei
             ,wt_co_f            as cookie
             ,wt_prov            as prov_id
             ,nvl(t4.name,'未知') as prov_name
             ,wt_city            as city_name
             ,channelid          as channel_id
             ,c_ip               as c_ip
             ,cs_host            as cs_host
             ,trmnl_style        as trmnl_style
             ,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')             as app_version
             ,wt_av              as sys_version
             ,date_day           as create_date
             ,date_time          as create_datetime
             ,click_time         as click_datetime
             ,serial_id          as serial_id
             ,start_time         as start_time
             ,down_time          as down_time
             ,status             as status
             ,issue_range        as issue_range
             ,if_own_develop     as if_own_develop
             ,'个性业务-宽带专区' as frequency  --频道
             ,area_name
             ,case when (t2.frequency = '首页' or t2.frequency = '我的') and t2.area_name = 'icon功能区' then concat('icon(',area_location,')') else t2.area_location end as area_location
             ,cs_referer         as link_referer
             ,wt_es              as link_current
             ,case when nvl(t2.url,'')='' then '--' 
                   else t2.url end as url
             ,issue_id           as issue_id
             ,mark_id            as mark_id
             ,marke_name         as mark_name
             ,marketing_type     as mark_type
             ,advertype          as advertype
             ,case when wt_event like '2020bjjtzq_yxqy_%' then regexp_extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d',0)
                   when wt_event like 'sy_qy_%' then concat(regexp_extract(wt_event,'sy_qy_\\d+-\\d+',0),reverse(regexp_extract(reverse(wt_event),'\\d+-\\d+-\\d+',0))) 
                   else wt_event end as wt_event
             ,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')              as wt_ti
             ,wt_mc_id           as wt_mc_id
             ,wt_ac              as wt_ac
             ,nvl(t1.wt_ac_id,t2.wt_ac_id)    as wt_ac_id
             ,goods_id           as goods_id
             ,sku_id             as sku_id
             ,editor_provname    as editor_prov_name
             ,marketing_mode     as mark_mode
             ,if_prov_page       as if_prov_page
             ,icon_code          as icon_code
             ,login_type         as login_type
             ,is_webtends        as is_webtends
             ,if_safe_audit      as if_safe_audit
             ,editor             as editor
             ,editor_phone_num   as editor_phone_num
             ,pro_company        as pro_company
             ,template_id        as template_id
             ,'宽带专区'         as analy_type
             ,conf_chan
             ,point_type
             ,area_type
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
from(select date_time   
           ,c_ip        
           ,cs_host     
           ,wt_cid      
           ,wt_co_f     
           ,wt_city     
           ,wt_mobile   
           ,wt_event    
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
     from ham.dwd_client_event_di
     where dt= '${DT}'
     and wt_event like 'kdyw_%') t1
inner join(select serial_id           
                  ,start_time          
                  ,down_time           
                  ,status              
                  ,issue_range         
                  ,if_own_develop      
                  ,area_name           
                  ,area_location       
                  ,issue_id            
                  ,marke_name          
                  ,url                 
                  ,editor_provname     
                  ,frequency           
                  ,marketing_mode      
                  ,if_prov_page        
                  ,case when match(marketing_id,'_') > 0 then splitByChar('_',marketing_id)[1] else marketing_id end as marketing_id -- 20210916新增 兼容老版本工单                
                  ,icon_code            
                  ,login_type          
                  ,is_webtends         
                  ,if_safe_audit       
                  ,editor              
                  ,editor_phone_num    
                  ,pro_company         
                  ,template_id         
                  ,marketing_type      
                  ,wt_ac_id                
                  ,conf_chan
,case when (frequency = '首页' or frequency = '我的') and area_type = 'icon功能区' then 'icon区域'
when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'
else '其他' end as point_type
,area_type
           from ham.dim_client_ord_info_d
           where dt='${DT}'
           and frequency like '%宽带专区%'
           ) t2 --工单表
on splitByChar('_',t2.marketing_id)[1] = reverse(substr(reverse(t1.wt_event), 0, match(reverse(t1.wt_event), '_') - 1))
inner join ham.dim_client_h5 t3 --码表
on 1 = 1            
left join (select prov,name
           from ham.dim_client_city 
           group by prov,name) t4 
on t1.wt_prov = t4.prov                --地市翻译表                      
where locate(t3.event,t1.wt_event) > 0 
and t3.pindao = t2.frequency
;


-- 10、工单icon编码数据ham.ads_hachi_jzyy_xtb2_51008
insert into table ham.ads_hachi_jzyy_xtb2_51008
select  wt_mobile                    as bill_no
       ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as imei
       ,wt_co_f                      as cookie
       ,wt_prov                      as prov_id
       ,nvl(t4.name,'未知')            as prov_name
       ,wt_city                      as city_name
       ,channelid                    as channel_id
       ,c_ip                         as c_ip
       ,cs_host                      as cs_host
       ,trmnl_style                  as trmnl_style
       ,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                       as app_version
       ,wt_av                        as sys_version
       ,date_day                     as create_date
       ,date_time                    as create_datetime
       ,click_time                   as click_datetime
       ,serial_id                    as serial_id
       ,start_time                   as start_time
       ,down_time                    as down_time
       ,status                       as status
       ,issue_range                  as issue_range
       ,if_own_develop               as if_own_develop
       ,frequency
       ,area_name
       ,case when (t2.frequency = '首页' or t2.frequency = '我的') and t2.area_name = 'icon功能区' then concat('icon(',area_location,')')  else t2.area_location end as area_location
       ,cs_referer                   as link_referer
       ,wt_es                        as link_current
       ,case when nvl(t2.url,'')='' then '--'
             else t2.url end         as url
       ,issue_id                     as issue_id
       ,mark_id                      as mark_id
       ,marke_name                   as mark_name
       ,marketing_type               as mark_type
       ,advertype                    as advertype
       ,case when wt_event like '2020bjjtzq_yxqy_%' then regexp_extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d',0)
             when wt_event like 'sy_qy_%' then concat(regexp_extract(wt_event,'sy_qy_\\d+-\\d+',0),reverse(regexp_extract(reverse(wt_event),'\\d+-\\d+-\\d+',0)))  else wt_event end as wt_event
       ,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                        as wt_ti
       ,wt_mc_id                     as wt_mc_id
       ,wt_ac                        as wt_ac
       ,nvl(t1.wt_ac_id,t2.wt_ac_id) as wt_ac_id
       ,goods_id                     as goods_id
       ,sku_id                       as sku_id
       ,editor_provname              as editor_prov_name
       ,marketing_mode               as mark_mode
       ,if_prov_page                 as if_prov_page
       ,icon_code                    as icon_code
       ,login_type                   as login_type
       ,is_webtends                  as is_webtends
       ,if_safe_audit                as if_safe_audit
       ,editor                       as editor
       ,editor_phone_num             as editor_phone_num
       ,pro_company                  as pro_company
       ,template_id                  as template_id
       ,'工单icon编码'                   as analy_type
       ,conf_chan
       ,point_type
       ,area_type
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
from
(
	select  date_time
	       ,c_ip
	       ,cs_host
	       ,wt_cid
	       ,wt_co_f
	       ,wt_city
	       ,wt_mobile
	       ,wt_event
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
	from ham.dwd_client_event_di
	where dt= '${DT}'
	and trmnl_style = 'h5'
) as t1
inner join
(
	select  serial_id
	       ,start_time
	       ,down_time
	       ,status
	       ,issue_range
	       ,if_own_develop
	       ,area_name
	       ,area_location
	       ,issue_id
	       ,marke_name
	       ,url
	       ,editor_provname
	       ,frequency
	       ,marketing_mode
	       ,if_prov_page
	       ,marketing_id
	       ,icon_code
	       ,login_type
	       ,is_webtends
	       ,if_safe_audit
	       ,editor
	       ,editor_phone_num
	       ,pro_company
	       ,template_id
	       ,marketing_type
	       ,wt_ac_id
	       ,conf_chan
	       ,case when (frequency = '首页' or frequency = '我的') and area_type = 'icon功能区' then 'icon区域'
	             when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'  else '其他' end as point_type
	       ,area_type
	from ham.dim_client_ord_info_d
	where dt='${DT}'
        and frequency <> '首页' 
)t2
on t1.wt_event = t2.icon_code and t1.mark_id = splitByChar('_',t2.marketing_id)[1] --工单表
left join
(
	select  prov,name
	from ham.dim_client_city
	group by prov,name
) t4
on t1.wt_prov = t4.prov
;


-- 11、ios端advertype传参错为29数据修复ham.ads_hachi_jzyy_xtb2_51008
insert into table ham.ads_hachi_jzyy_xtb2_51008
select  wt_mobile                    as bill_no
       ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as imei
       ,wt_co_f                      as cookie
       ,wt_prov                      as prov_id
       ,nvl(t4.name,'未知')            as prov_name
       ,wt_city                      as city_name
       ,channelid                    as channel_id
       ,c_ip                         as c_ip
       ,cs_host                      as cs_host
       ,trmnl_style                  as trmnl_style
       ,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                       as app_version
       ,wt_av                        as sys_version
       ,date_day                     as create_date
       ,date_time                    as create_datetime
       ,click_time                   as click_datetime
       ,serial_id                    as serial_id
       ,start_time                   as start_time
       ,down_time                    as down_time
       ,status                       as status
       ,issue_range                  as issue_range
       ,if_own_develop               as if_own_develop
       ,frequency
       ,area_name
       ,case when (t2.frequency = '首页' or t2.frequency = '我的') and t2.area_name = 'icon功能区' then concat('icon(',area_location,')')  else t2.area_location end as area_location
       ,cs_referer                   as link_referer
       ,wt_es                        as link_current
       ,case when nvl(t2.url,'')='' then '--'
             else t2.url end         as url
       ,issue_id                     as issue_id
       ,mark_id                      as mark_id
       ,marke_name                   as mark_name
       ,marketing_type               as mark_type
       ,advertype                    as advertype
       ,case when wt_event like '2020bjjtzq_yxqy_%' then regexp_extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d',0)
             when wt_event like 'sy_qy_%' then concat(regexp_extract(wt_event,'sy_qy_\\d+-\\d+',0),reverse(regexp_extract(reverse(wt_event),'\\d+-\\d+-\\d+',0)))  else wt_event end as wt_event
       ,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                        as wt_ti
       ,wt_mc_id                     as wt_mc_id
       ,wt_ac                        as wt_ac
       ,nvl(t1.wt_ac_id,t2.wt_ac_id) as wt_ac_id
       ,goods_id                     as goods_id
       ,sku_id                       as sku_id
       ,editor_provname              as editor_prov_name
       ,marketing_mode               as mark_mode
       ,if_prov_page                 as if_prov_page
       ,icon_code                    as icon_code
       ,login_type                   as login_type
       ,is_webtends                  as is_webtends
       ,if_safe_audit                as if_safe_audit
       ,editor                       as editor
       ,editor_phone_num             as editor_phone_num
       ,pro_company                  as pro_company
       ,template_id                  as template_id
       ,'advertype误传修正'              as analy_type
       ,conf_chan
       ,point_type
       ,area_type
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
from
(
	select  date_time
	       ,c_ip
	       ,cs_host
	       ,wt_cid
	       ,wt_co_f
	       ,wt_city
	       ,wt_mobile
	       ,wt_event
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
	from ham.dwd_client_event_di a
	where dt= '${DT}'
	and trmnl_style = 'ios'
	and advertype = '29'
	and not exists(
	select  marketing_id
	from ham.dim_client_ord_info_d b
	where b.dt='${DT}'
	and concat(a.advertype, '_', a.mark_id) = b.marketing_id) 
) t1
inner join
(
	select  serial_id
	       ,start_time
	       ,down_time
	       ,status
	       ,issue_range
	       ,if_own_develop
	       ,area_name
	       ,area_location
	       ,issue_id
	       ,marke_name
	       ,url
	       ,editor_provname
	       ,frequency
	       ,marketing_mode
	       ,if_prov_page
	       ,marketing_id
	       ,icon_code
	       ,login_type
	       ,is_webtends
	       ,if_safe_audit
	       ,editor
	       ,editor_phone_num
	       ,pro_company
	       ,template_id
	       ,marketing_type
	       ,wt_ac_id
	       ,conf_chan
	       ,case when (frequency = '首页' or frequency = '我的') and area_type = 'icon功能区' then 'icon区域'
	             when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'  else '其他' end as point_type
	       ,area_type
	from ham.dim_client_ord_info_d
	where dt='${DT}' 
) t2 --工单表
on concat('522_', t1.mark_id) = t2.marketing_id
left join
(
	select  prov,name
	from ham.dim_client_city
	group by prov,name 
) t4
on t1.wt_prov = t4.prov --地市翻译表 
;



-- 12、ios端7.5.0版本数据修复ham.ads_hachi_jzyy_xtb2_51008
insert into table ham.ads_hachi_jzyy_xtb2_51008 
select  wt_mobile                    as bill_no
       ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as imei
       ,wt_co_f                      as cookie
       ,wt_prov                      as prov_id
       ,nvl(t4.name,'未知')            as prov_name
       ,wt_city                      as city_name
       ,channelid                    as channel_id
       ,c_ip                         as c_ip
       ,cs_host                      as cs_host
       ,trmnl_style                  as trmnl_style
       ,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                       as app_version
       ,wt_av                        as sys_version
       ,date_day                     as create_date
       ,date_time                    as create_datetime
       ,click_time                   as click_datetime
       ,serial_id                    as serial_id
       ,start_time                   as start_time
       ,down_time                    as down_time
       ,status                       as status
       ,issue_range                  as issue_range
       ,if_own_develop               as if_own_develop
       ,frequency
       ,area_name
       ,case when (t2.frequency = '首页' or t2.frequency = '我的') and t2.area_name = 'icon功能区' then concat('icon(',area_location,')')  else t2.area_location end as area_location
       ,cs_referer                   as link_referer
       ,wt_es                        as link_current
       ,case when nvl(t2.url,'')='' then '--'
             else t2.url end         as url
       ,issue_id                     as issue_id
       ,mark_id                      as mark_id
       ,marke_name                   as mark_name
       ,marketing_type               as mark_type
       ,advertype                    as advertype
       ,case when wt_event like '2020bjjtzq_yxqy_%' then regexp_extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d',0)
             when wt_event like 'sy_qy_%' then concat(regexp_extract(wt_event,'sy_qy_\\d+-\\d+',0),reverse(regexp_extract(reverse(wt_event),'\\d+-\\d+-\\d+',0)))  else wt_event end as wt_event
       ,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                        as wt_ti
       ,wt_mc_id                     as wt_mc_id
       ,wt_ac                        as wt_ac
       ,nvl(t1.wt_ac_id,t2.wt_ac_id) as wt_ac_id
       ,goods_id                     as goods_id
       ,sku_id                       as sku_id
       ,editor_provname              as editor_prov_name
       ,marketing_mode               as mark_mode
       ,if_prov_page                 as if_prov_page
       ,icon_code                    as icon_code
       ,login_type                   as login_type
       ,is_webtends                  as is_webtends
       ,if_safe_audit                as if_safe_audit
       ,editor                       as editor
       ,editor_phone_num             as editor_phone_num
       ,pro_company                  as pro_company
       ,template_id                  as template_id
       ,'ios端7.5.0版本修正数据'            as analy_type
       ,conf_chan
       ,point_type
       ,area_type
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
from
(
	select  date_time
	       ,c_ip
	       ,cs_host
	       ,wt_cid
	       ,wt_co_f
	       ,wt_city
	       ,wt_mobile
	       ,wt_event
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
	from ham.dwd_client_event_di
	where dt= '${DT}'
	and trmnl_style = 'ios'
	and wt_av = 'app_ios_v7.5.0'
	and mark_id = '522'
) t1
inner join
(
	select  serial_id
	       ,start_time
	       ,down_time
	       ,status
	       ,issue_range
	       ,if_own_develop
	       ,area_name
	       ,area_location
	       ,issue_id
	       ,marke_name
	       ,url
	       ,editor_provname
	       ,frequency
	       ,marketing_mode
	       ,if_prov_page
	       ,marketing_id
	       ,icon_code
	       ,login_type
	       ,is_webtends
	       ,if_safe_audit
	       ,editor
	       ,editor_phone_num
	       ,pro_company
	       ,template_id
	       ,marketing_type
	       ,wt_ac_id
	       ,conf_chan
	       ,case when (frequency = '首页' or frequency = '我的') and area_type = 'icon功能区' then 'icon区域'
	             when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'  else '其他' end as point_type
	       ,area_type
	from ham.dim_client_ord_info_d
	where dt ='${DT}'
	and frequency = '我的页面（v6.6）'
	and area_name = '我的页面icon区域'
) t2 -- 工单表
on concat('522_', t1.advertype) = t2.marketing_id
left join
(
	select  prov,name
	from ham.dim_client_city -- 地市翻译表
	group by prov,name
) t4
on t1.wt_prov = t4.prov
;
