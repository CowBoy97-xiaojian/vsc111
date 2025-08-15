truncate table ham_jituan.ads_hachi_jzyy_xtb2_51008;

-- 有重复工单
drop table ptemp.DW_C_CTH_CM_LOG_D_01;
create table ptemp.DW_C_CTH_CM_LOG_D_01
as select SERIAL_ID           
,START_TIME          
,DOWN_TIME           
,STATUS              
,ISSUE_RANGE         
,IF_OWN_DEVELOP      
,AREA_NAME           
,AREA_LOCATION       
,ISSUE_ID            
,MARKE_NAME          
,URL                 
,EDITOR_PROVNAME     
,a.FREQUENCY           
,MARKETING_MODE      
,IF_PROV_PAGE        
,a.marketing_id
,ICON_CODE            
,LOGIN_TYPE          
,IS_WEBTENDS         
,IF_SAFE_AUDIT       
,EDITOR              
,EDITOR_PHONE_NUM    
,PRO_COMPANY         
,TEMPLATE_ID         
,MARKETING_TYPE      
,WT_AC_ID   
,CONF_CHAN
,AREA_TYPE
from(select SERIAL_ID           
,START_TIME          
,DOWN_TIME           
,STATUS              
,ISSUE_RANGE         
,IF_OWN_DEVELOP      
,area_name       
,AREA_LOCATION 
,ISSUE_ID 
,MARKE_NAME 
,URL 
,EDITOR_PROVNAME
,frequency  
,MARKETING_MODE
,IF_PROV_PAGE
,case when instr(marketing_id,'_') > 0 then split(marketing_id,'_')[1] else marketing_id end as marketing_id  -- 20210916修改，增加历史数据兼容
,ICON_CODE
,LOGIN_TYPE
,IS_WEBTENDS         
,IF_SAFE_AUDIT
,EDITOR
,EDITOR_PHONE_NUM
,PRO_COMPANY
,TEMPLATE_ID
,MARKETING_TYPE
,WT_AC_ID
,CONF_CHAN
,AREA_TYPE
from ham_jituan.dim_client_ord_info_d 
where dt='${DT}') a
inner join(select FREQUENCY,marketing_id,count(1) as pv 
from(select START_TIME, DOWN_TIME, EDITOR_PROVNAME, 
area_name, AREA_LOCATION, MARKE_NAME, URL,
frequency, MARKETING_MODE, 
case when instr(marketing_id,'_') > 0 then split(marketing_id,'_')[1] else marketing_id end as marketing_id  -- 20210916修改，增加历史数据兼容
,dt,count(1) as pv 
from ham_jituan.dim_client_ord_info_d 
where dt = '${DT}'
group by START_TIME, DOWN_TIME, EDITOR_PROVNAME, AREA_NAME, AREA_LOCATION, MARKE_NAME, URL,FREQUENCY, MARKETING_MODE, MARKETING_ID, dt) a           
group by FREQUENCY
,marketing_id 
having pv > 1) b --判断重复频道，如果有 增加匹配条件
on a.FREQUENCY=b.FREQUENCY
and a.marketing_id=b.marketing_id
;


-- 无重复工单
drop table ptemp.DW_C_CTH_CM_LOG_D_02;
create table ptemp.DW_C_CTH_CM_LOG_D_02
as select  SERIAL_ID           
,START_TIME          
,DOWN_TIME           
,STATUS              
,ISSUE_RANGE         
,IF_OWN_DEVELOP      
,AREA_NAME           
,AREA_LOCATION       
,ISSUE_ID            
,MARKE_NAME          
,URL                 
,EDITOR_PROVNAME     
,a.FREQUENCY           
,MARKETING_MODE      
,IF_PROV_PAGE        
,a.marketing_id
,ICON_CODE            
,LOGIN_TYPE          
,IS_WEBTENDS         
,IF_SAFE_AUDIT       
,EDITOR              
,EDITOR_PHONE_NUM    
,PRO_COMPANY         
,TEMPLATE_ID         
,MARKETING_TYPE      
,WT_AC_ID   
,CONF_CHAN
,AREA_TYPE
from(select SERIAL_ID           
,START_TIME          
,DOWN_TIME           
,STATUS              
,ISSUE_RANGE         
,IF_OWN_DEVELOP      
,area_name       
,AREA_LOCATION 
,ISSUE_ID 
,MARKE_NAME 
,URL 
,EDITOR_PROVNAME
,frequency  
,MARKETING_MODE
,IF_PROV_PAGE
,case when instr(marketing_id,'_') > 0 then split(marketing_id,'_')[1] else marketing_id end as marketing_id  -- 20210916修改，增加历史数据兼容
,ICON_CODE
,LOGIN_TYPE
,IS_WEBTENDS         
,IF_SAFE_AUDIT
,EDITOR
,EDITOR_PHONE_NUM
,PRO_COMPANY
,TEMPLATE_ID
,MARKETING_TYPE
,WT_AC_ID
,CONF_CHAN
,AREA_TYPE
from ham_jituan.dim_client_ord_info_d 
where dt='${DT}') a
inner join(select FREQUENCY,marketing_id,count(1) as pv 
from(select START_TIME, DOWN_TIME, EDITOR_PROVNAME, 
area_name, AREA_LOCATION, MARKE_NAME, URL,
frequency, MARKETING_MODE, 
case when instr(marketing_id,'_') > 0 then split(marketing_id,'_')[1] else marketing_id end as marketing_id  -- 20210916修改，增加历史数据兼容
,dt,count(1) as pv 
from ham_jituan.dim_client_ord_info_d 
where dt = '${DT}'
group by START_TIME, DOWN_TIME, EDITOR_PROVNAME, AREA_NAME, AREA_LOCATION, MARKE_NAME, URL,FREQUENCY, MARKETING_MODE, MARKETING_ID, dt) a           
group by FREQUENCY
,marketing_id 
having pv = 1) b --判断重复频道，如果有 增加匹配条件
on a.FREQUENCY=b.FREQUENCY
and a.marketing_id=b.marketing_id
;


-- h5_工单重复部分
insert into table ham_jituan.ads_hachi_jzyy_xtb2_51008 
select        wt_mobile          as BILL_NO
,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')  as IMEI
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
,FREQUENCY  
,area_name          as area_name
,case when (t2.FREQUENCY = '首页' or t2.FREQUENCY = '我的') and t2.AREA_NAME = 'ICON功能区' then concat('ICON(',AREA_LOCATION,')') 
else t2.AREA_LOCATION end as area_location
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
,If_prov_page       as If_prov_page
,icon_code          as icon_code
,login_type         as login_type
,is_webtends        as is_webtends
,if_safe_audit      as if_safe_audit
,editor             as editor
,editor_phone_num   as editor_phone_num
,pro_company        as pro_company
,template_id        as template_id
,'h5_工单重复部分'  as ANALY_TYPE
,CONF_CHAN
,POINT_TYPE
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
from ham_jituan.dwd_client_event_di
where dt= '${DT}'
and trmnl_style = 'H5') t1
inner join(select  SERIAL_ID           
,START_TIME          
,DOWN_TIME           
,STATUS              
,ISSUE_RANGE         
,IF_OWN_DEVELOP      
,AREA_NAME           
,AREA_LOCATION       
,ISSUE_ID            
,MARKE_NAME          
,URL                 
,EDITOR_PROVNAME     
,FREQUENCY           
,MARKETING_MODE      
,IF_PROV_PAGE        
,MARKETING_ID        
,ICON_CODE            
,LOGIN_TYPE          
,IS_WEBTENDS         
,IF_SAFE_AUDIT       
,EDITOR              
,EDITOR_PHONE_NUM    
,PRO_COMPANY         
,TEMPLATE_ID         
,MARKETING_TYPE      
,WT_AC_ID   
,CONF_CHAN
,case when (frequency = '首页' or frequency = '我的') and area_type = 'ICON功能区' then 'ICON区域'
when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'
else '其他' end as POINT_TYPE
,AREA_TYPE
from PTEMP.DW_C_CTH_CM_LOG_D_01) t2 --工单表
on t1.mark_id = t2.marketing_id  
inner join ham_jituan.dim_client_h5 t3 --码表
on t2.AREA_NAME = t3.quyu             
left join (select prov,name
from ham_jituan.dim_client_city 
group by prov,name) t4 
on t1.wt_prov = t4.prov                --地市翻译表                      
where locate(t3.event,t1.wt_event) > 0 
and t3.pindao = t2.FREQUENCY
;


-- H5新版本数据
insert into table ham_jituan.ads_hachi_jzyy_xtb2_51008 
select        wt_mobile          as BILL_NO
,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as IMEI
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
,FREQUENCY  
,area_name
,case when (t2.FREQUENCY = '首页' or t2.FREQUENCY = '我的') and t2.AREA_NAME = 'ICON功能区' then concat('ICON(',AREA_LOCATION,')') 
else t2.AREA_LOCATION end as area_location
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
,If_prov_page       as If_prov_page
,icon_code          as icon_code
,login_type         as login_type
,is_webtends        as is_webtends
,if_safe_audit      as if_safe_audit
,editor             as editor
,editor_phone_num   as editor_phone_num
,pro_company        as pro_company
,template_id        as template_id
,'h5新版本' as ANALY_TYPE 
,CONF_CHAN
,POINT_TYPE
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
from ham_jituan.dwd_client_event_di
where dt= '${DT}' ) as t1
inner join (select SERIAL_ID           
,START_TIME          
,DOWN_TIME           
,STATUS              
,ISSUE_RANGE         
,IF_OWN_DEVELOP      
,area_name       
,AREA_LOCATION 
,ISSUE_ID 
,MARKE_NAME 
,URL 
,EDITOR_PROVNAME
,frequency  
,MARKETING_MODE
,IF_PROV_PAGE
,marketing_id
,ICON_CODE
,LOGIN_TYPE
,IS_WEBTENDS         
,IF_SAFE_AUDIT
,EDITOR
,EDITOR_PHONE_NUM
,PRO_COMPANY
,TEMPLATE_ID
,MARKETING_TYPE
,WT_AC_ID
,CONF_CHAN
,case when (frequency = '首页' or frequency = '我的') and area_type = 'ICON功能区' then 'ICON区域'
when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'
else '其他' end as POINT_TYPE
,AREA_TYPE 
from ham_jituan.dim_client_ord_info_d 
where dt='${DT}' )t2 on t1.wt_event = t2.marketing_id --工单表
left join (select prov,name from ham_jituan.dim_client_city  group by prov,name) t4 on t1.wt_prov = t4.prov
where trmnl_style = 'H5'
;


-- 原生
insert into table ham_jituan.ads_hachi_jzyy_xtb2_51008 
select        wt_mobile          as BILL_NO
,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as IMEI
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
,FREQUENCY  
,area_name
,case when (t2.FREQUENCY = '首页' or t2.FREQUENCY = '我的') and t2.AREA_NAME = 'ICON功能区' then concat('ICON(',AREA_LOCATION,')') 
else t2.AREA_LOCATION end as area_location
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
,If_prov_page       as If_prov_page
,icon_code          as icon_code
,login_type         as login_type
,is_webtends        as is_webtends
,if_safe_audit      as if_safe_audit
,editor             as editor
,editor_phone_num   as editor_phone_num
,pro_company        as pro_company
,template_id        as template_id
,'原生_工单'  as ANALY_TYPE
,CONF_CHAN
,POINT_TYPE
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
from ham_jituan.dwd_client_event_di
where dt= '${DT}'
and trmnl_style in ('ANDROID','IOS')) t1
inner join(select  SERIAL_ID           
,START_TIME          
,DOWN_TIME           
,STATUS              
,ISSUE_RANGE         
,IF_OWN_DEVELOP      
,AREA_NAME           
,AREA_LOCATION       
,ISSUE_ID            
,MARKE_NAME          
,URL                 
,EDITOR_PROVNAME  
,FREQUENCY 
,MARKETING_MODE      
,IF_PROV_PAGE        
,MARKETING_ID        
,ICON_CODE            
,LOGIN_TYPE          
,IS_WEBTENDS         
,IF_SAFE_AUDIT       
,EDITOR              
,EDITOR_PHONE_NUM    
,PRO_COMPANY         
,TEMPLATE_ID         
,MARKETING_TYPE      
,WT_AC_ID   
,CONF_CHAN
,case when (frequency = '首页' or frequency = '我的') and area_type = 'ICON功能区' then 'ICON区域'
when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'
else '其他' end as POINT_TYPE
,AREA_TYPE 
from ham_jituan.dim_client_ord_info_d where dt='${DT}' ) t2 --工单表
on  concat(t1.advertype,'_',t1.mark_id) = t2.marketing_id  
left join (select prov,name
from ham_jituan.dim_client_city 
group by prov,name) t4 
on t1.wt_prov = t4.prov                --地市翻译表
;


-- 自定义原生部分
insert into table ham_jituan.ads_hachi_jzyy_xtb2_51008 
select wt_mobile          as BILL_NO
      ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as IMEI
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
      ,t2.FREQUENCY
      ,area_name
      ,t2.AREA_LOCATION
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
      ,'--'               as If_prov_page
      ,'--'               as icon_code
      ,'--'               as login_type
      ,'--'               as is_webtends
      ,'--'               as if_safe_audit
      ,'--'               as editor
      ,'--'               as editor_phone_num
      ,'--'               as pro_company
      ,'--'               as template_id
      ,'自定义原生部分'        as ANALY_TYPE
      ,CONF_CHAN          as CONF_CHAN
   ,POINT_TYPE         as POINT_TYPE
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
	   ,CASE WHEN (wt_event like 'qwbn2110150001_%' or wt_event like 'qwbn2110150002_%') THEN split(wt_event,'_')[0]  ELSE wt_event END AS wt_event    
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
     from ham_jituan.dwd_client_event_di
     where dt= '${DT}'
     and trmnl_style in ('ANDROID','IOS')) t1
inner join(select  '一级渠道'     AS conf_chan
	       ,frequency     AS frequency
	       ,'--'          AS area_type
	       ,area_name     AS area_name
	       ,marke_name    AS area_location
	       ,marke_name    AS marke_name
	       ,'普通全网配置类'          AS marketing_mode
	       ,'未知'        AS POINT_TYPE
	       ,event
           from ham_jituan.dim_client_ld_zdy_d
where dt='${DT}'
) t2 
on t1.wt_event = t2.event  -- 自定义码表           
left join (select prov,name
           from ham_jituan.dim_client_city 
           group by prov,name) t4 
on t1.wt_prov = t4.prov
;


-- h5_工单无重复部分
insert into table ham_jituan.ads_hachi_jzyy_xtb2_51008 
select        wt_mobile          as BILL_NO
             ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as IMEI
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
             ,FREQUENCY 
             ,area_name          as area_name
             ,case when (t2.FREQUENCY = '首页' or t2.FREQUENCY = '我的') and t2.AREA_NAME = 'ICON功能区' then concat('ICON(',AREA_LOCATION,')') 
                   else t2.AREA_LOCATION end as area_location
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
             ,If_prov_page       as If_prov_page
             ,icon_code          as icon_code
             ,login_type         as login_type
             ,is_webtends        as is_webtends
             ,if_safe_audit      as if_safe_audit
             ,editor             as editor
             ,editor_phone_num   as editor_phone_num
             ,pro_company        as pro_company
             ,template_id        as template_id
             ,'h5_工单无重复'    as ANALY_TYPE
             ,CONF_CHAN
             ,POINT_TYPE
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
     from ham_jituan.dwd_client_event_di
     where dt= '${DT}'
     and trmnl_style = 'H5') t1
inner join(select  SERIAL_ID           
                  ,START_TIME          
                  ,DOWN_TIME           
                  ,STATUS              
                  ,ISSUE_RANGE         
                  ,IF_OWN_DEVELOP      
                  ,AREA_NAME           
                  ,AREA_LOCATION       
                  ,ISSUE_ID            
                  ,MARKE_NAME          
                  ,URL                 
                  ,EDITOR_PROVNAME     
                  ,FREQUENCY           
                  ,MARKETING_MODE      
                  ,IF_PROV_PAGE        
                  ,MARKETING_ID        
                  ,ICON_CODE            
                  ,LOGIN_TYPE          
                  ,IS_WEBTENDS         
                  ,IF_SAFE_AUDIT       
                  ,EDITOR              
                  ,EDITOR_PHONE_NUM    
                  ,PRO_COMPANY         
                  ,TEMPLATE_ID         
                  ,MARKETING_TYPE      
                  ,WT_AC_ID   
               ,CONF_CHAN
,case when (frequency = '首页' or frequency = '我的') and area_type = 'ICON功能区' then 'ICON区域'
when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'
else '其他' end as POINT_TYPE
,AREA_TYPE 
           from ptemp.DW_C_CTH_CM_LOG_D_02) t2 --工单表
on t1.mark_id = t2.marketing_id  
inner join ham_jituan.dim_client_h5 t3 --码表
on 1 = 1            
left join (select prov,name
           from ham_jituan.dim_client_city 
           group by prov,name) t4 
on t1.wt_prov = t4.prov                --地市翻译表                      
where locate(t3.event,t1.wt_event) > 0 
and t3.pindao = t2.FREQUENCY
;

-- 自定义H5部分
INSERT INTO TABLE ham_jituan.ads_hachi_jzyy_xtb2_51008 
SELECT  wt_mobile         AS BILL_NO
       ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') AS IMEI
       ,wt_co_f           AS cookie
       ,wt_prov           AS prov_id
       ,nvl(t4.name,'未知') AS prov_name
       ,wt_city           AS city_name
       ,channelid         AS channel_id
       ,c_ip              AS c_ip
       ,cs_host           AS cs_host
       ,trmnl_style       AS trmnl_style
       ,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')            AS app_version
       ,wt_av             AS sys_version
       ,date_day               AS create_date
       ,date_time         AS create_datetime
       ,click_time        AS click_datetime
       ,'--'              AS serial_id
       ,'--'              AS start_time
       ,'--'              AS down_time
       ,'--'              AS status
       ,'--'              AS issue_range
       ,'--'              AS if_own_develop
       ,t2.FREQUENCY
       ,area_name
       ,t2.AREA_LOCATION
       ,'--'              AS link_referer
       ,'--'              AS link_current
       ,'--'              AS url
       ,'--'              AS issue_id
       ,mark_id           AS mark_id
       ,marke_name        AS mark_name
       ,'--'              AS mark_type
       ,advertype         AS advertype
       ,wt_event
       ,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')             AS wt_ti
       ,wt_mc_id          AS wt_mc_id
       ,wt_ac             AS wt_ac
       ,t1.wt_ac_id       AS wt_ac_id
       ,goods_id          AS goods_id
       ,sku_id            AS sku_id
       ,'--'              AS editor_prov_name
       ,'--'              AS mark_mode
       ,'--'              AS If_prov_page
       ,'--'              AS icon_code
       ,'--'              AS login_type
       ,'--'              AS is_webtends
       ,'--'              AS if_safe_audit
       ,'--'              AS editor
       ,'--'              AS editor_phone_num
       ,'--'              AS pro_company
       ,'--'              AS template_id
       ,'自定义H5部分'           AS ANALY_TYPE
       ,CONF_CHAN         AS CONF_CHAN
       ,POINT_TYPE        AS POINT_TYPE
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
FROM
(
	SELECT  date_time
	       ,c_ip
	       ,cs_host
	       ,wt_cid
	       ,wt_co_f
	       ,wt_city
	       ,wt_mobile
	       ,CASE WHEN (wt_event like 'qwbn2110150001_%' or wt_event like 'qwbn2110150002_%') THEN split(wt_event,'_')[0]  ELSE wt_event END AS wt_event
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
	FROM ham_jituan.dwd_client_event_di
	WHERE dt= '${DT}'
	AND trmnl_style IN ('H5')
       AND advertype = 122
) t1
INNER JOIN
(
	SELECT  '一级渠道'     AS conf_chan
	       ,frequency     AS frequency
	       ,'--'          AS area_type
	       ,area_name     AS area_name
	       ,marke_name    AS area_location
	       ,marke_name    AS marke_name
	       ,'普通全网配置类'          AS marketing_mode
	       ,'未知'        AS POINT_TYPE
	       ,event
              ,mark_id   as wt_mark_id
	FROM ham_jituan.dim_client_ld_zdy_d
       where dt='${DT}'
) t2
ON t1.mark_id = t2.wt_mark_id -- 自定义事件码表日表
LEFT JOIN
(
	SELECT  prov,name
	FROM ham_jituan.dim_client_city 
	group by prov,name
) t4
ON t1.wt_prov = t4.prov -- 地市翻译表 
;


-- 宽带专区
insert into table ham_jituan.ads_hachi_jzyy_xtb2_51008 
select        wt_mobile          as BILL_NO
             ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') as IMEI
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
             ,'个性业务-宽带专区' as FREQUENCY  --频道
             ,area_name
             ,case when (t2.FREQUENCY = '首页' or t2.FREQUENCY = '我的') and t2.AREA_NAME = 'ICON功能区' then concat('ICON(',AREA_LOCATION,')') else t2.AREA_LOCATION end as AREA_LOCATION
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
             ,If_prov_page       as If_prov_page
             ,icon_code          as icon_code
             ,login_type         as login_type
             ,is_webtends        as is_webtends
             ,if_safe_audit      as if_safe_audit
             ,editor             as editor
             ,editor_phone_num   as editor_phone_num
             ,pro_company        as pro_company
             ,template_id        as template_id
             ,'宽带专区'         as ANALY_TYPE
             ,CONF_CHAN
             ,POINT_TYPE
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
     from ham_jituan.dwd_client_event_di
     where dt= '${DT}'
     and wt_event like 'kdyw_%') t1
inner join(select SERIAL_ID           
                  ,START_TIME          
                  ,DOWN_TIME           
                  ,STATUS              
                  ,ISSUE_RANGE         
                  ,IF_OWN_DEVELOP      
                  ,AREA_NAME           
                  ,AREA_LOCATION       
                  ,ISSUE_ID            
                  ,MARKE_NAME          
                  ,URL                 
                  ,EDITOR_PROVNAME     
                  ,FREQUENCY           
                  ,MARKETING_MODE      
                  ,IF_PROV_PAGE        
                  ,case when instr(marketing_id,'_') > 0 then split(marketing_id,'_')[1] else marketing_id end as marketing_id -- 20210916新增 兼容老版本工单                
                  ,ICON_CODE            
                  ,LOGIN_TYPE          
                  ,IS_WEBTENDS         
                  ,IF_SAFE_AUDIT       
                  ,EDITOR              
                  ,EDITOR_PHONE_NUM    
                  ,PRO_COMPANY         
                  ,TEMPLATE_ID         
                  ,MARKETING_TYPE      
                  ,WT_AC_ID                
                  ,CONF_CHAN
,case when (frequency = '首页' or frequency = '我的') and area_type = 'ICON功能区' then 'ICON区域'
when frequency = '其他辅助广告位' or (frequency = '首页' and (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' and (area_type = '轮播图' or area_type = '营销区域')) then '广告位'
else '其他' end as POINT_TYPE
,AREA_TYPE
           from ham_jituan.dim_client_ord_info_d
           where dt='${DT}'
           and FREQUENCY like '%宽带专区%'
           ) t2 --工单表
on split(t2.marketing_id,'_')[1] = reverse(substr(reverse(t1.wt_event), 0, instr(reverse(t1.wt_event), '_') - 1))
inner join ham_jituan.dim_client_h5 t3 --码表
on 1 = 1            
left join (select prov,name
           from ham_jituan.dim_client_city 
           group by prov,name) t4 
on t1.wt_prov = t4.prov                --地市翻译表                      
where locate(t3.event,t1.wt_event) > 0 
and t3.pindao = t2.FREQUENCY
;


-- 工单ICON编码数据
INSERT INTO TABLE ham_jituan.ads_hachi_jzyy_xtb2_51008
SELECT  wt_mobile                    AS BILL_NO
       ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') AS IMEI
       ,wt_co_f                      AS cookie
       ,wt_prov                      AS prov_id
       ,nvl(t4.name,'未知')            AS prov_name
       ,wt_city                      AS city_name
       ,channelid                    AS channel_id
       ,c_ip                         AS c_ip
       ,cs_host                      AS cs_host
       ,trmnl_style                  AS trmnl_style
       ,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                       AS app_version
       ,wt_av                        AS sys_version
       ,date_day                     AS create_date
       ,date_time                    AS create_datetime
       ,click_time                   AS click_datetime
       ,serial_id                    AS serial_id
       ,start_time                   AS start_time
       ,down_time                    AS down_time
       ,status                       AS status
       ,issue_range                  AS issue_range
       ,if_own_develop               AS if_own_develop
       ,FREQUENCY
       ,area_name
       ,CASE WHEN (t2.FREQUENCY = '首页' or t2.FREQUENCY = '我的') AND t2.AREA_NAME = 'ICON功能区' THEN concat('ICON(',AREA_LOCATION,')')  ELSE t2.AREA_LOCATION END AS area_location
       ,cs_referer                   AS link_referer
       ,wt_es                        AS link_current
       ,CASE WHEN nvl(t2.url,'')='' THEN '--'
             ELSE t2.url END         AS url
       ,issue_id                     AS issue_id
       ,mark_id                      AS mark_id
       ,marke_name                   AS mark_name
       ,marketing_type               AS mark_type
       ,advertype                    AS advertype
       ,CASE WHEN wt_event like '2020bjjtzq_yxqy_%' THEN regexp_extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d',0)
             WHEN wt_event like 'sy_qy_%' THEN concat(regexp_extract(wt_event,'sy_qy_\\d+-\\d+',0),reverse(regexp_extract(reverse(wt_event),'\\d+-\\d+-\\d+',0)))  ELSE wt_event END AS wt_event
       ,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                        AS wt_ti
       ,wt_mc_id                     AS wt_mc_id
       ,wt_ac                        AS wt_ac
       ,nvl(t1.wt_ac_id,t2.wt_ac_id) AS wt_ac_id
       ,goods_id                     AS goods_id
       ,sku_id                       AS sku_id
       ,editor_provname              AS editor_prov_name
       ,marketing_mode               AS mark_mode
       ,If_prov_page                 AS If_prov_page
       ,icon_code                    AS icon_code
       ,login_type                   AS login_type
       ,is_webtends                  AS is_webtends
       ,if_safe_audit                AS if_safe_audit
       ,editor                       AS editor
       ,editor_phone_num             AS editor_phone_num
       ,pro_company                  AS pro_company
       ,template_id                  AS template_id
       ,'工单ICON编码'                   AS ANALY_TYPE
       ,CONF_CHAN
       ,POINT_TYPE
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
FROM
(
	SELECT  date_time
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
	FROM ham_jituan.dwd_client_event_di
	WHERE dt= '${DT}'
	AND trmnl_style = 'H5'
) AS t1
INNER JOIN
(
	SELECT  SERIAL_ID
	       ,START_TIME
	       ,DOWN_TIME
	       ,STATUS
	       ,ISSUE_RANGE
	       ,IF_OWN_DEVELOP
	       ,area_name
	       ,AREA_LOCATION
	       ,ISSUE_ID
	       ,MARKE_NAME
	       ,URL
	       ,EDITOR_PROVNAME
	       ,frequency
	       ,MARKETING_MODE
	       ,IF_PROV_PAGE
	       ,marketing_id
	       ,ICON_CODE
	       ,LOGIN_TYPE
	       ,IS_WEBTENDS
	       ,IF_SAFE_AUDIT
	       ,EDITOR
	       ,EDITOR_PHONE_NUM
	       ,PRO_COMPANY
	       ,TEMPLATE_ID
	       ,MARKETING_TYPE
	       ,WT_AC_ID
	       ,CONF_CHAN
	       ,CASE WHEN (frequency = '首页' or frequency = '我的') AND area_type = 'ICON功能区' THEN 'ICON区域'
	             WHEN frequency = '其他辅助广告位' or (frequency = '首页' AND (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' AND (area_type = '轮播图' or area_type = '营销区域')) THEN '广告位'  ELSE '其他' END AS POINT_TYPE
	       ,AREA_TYPE
	FROM ham_jituan.dim_client_ord_info_d
	WHERE dt='${DT}'
        and frequency <> '首页' 
)t2
ON t1.wt_event = t2.ICON_CODE AND t1.mark_id = split(t2.marketing_id, '_')[1] --工单表
LEFT JOIN
(
	SELECT  prov,name
	FROM ham_jituan.dim_client_city
	group by prov,name
) t4
ON t1.wt_prov = t4.prov
;


-- ios端advertype传参错为29数据修复
INSERT INTO TABLE ham_jituan.ads_hachi_jzyy_xtb2_51008
SELECT  wt_mobile                    AS BILL_NO
       ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') AS IMEI
       ,wt_co_f                      AS cookie
       ,wt_prov                      AS prov_id
       ,nvl(t4.name,'未知')            AS prov_name
       ,wt_city                      AS city_name
       ,channelid                    AS channel_id
       ,c_ip                         AS c_ip
       ,cs_host                      AS cs_host
       ,trmnl_style                  AS trmnl_style
       ,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                       AS app_version
       ,wt_av                        AS sys_version
       ,date_day                     AS create_date
       ,date_time                    AS create_datetime
       ,click_time                   AS click_datetime
       ,serial_id                    AS serial_id
       ,start_time                   AS start_time
       ,down_time                    AS down_time
       ,status                       AS status
       ,issue_range                  AS issue_range
       ,if_own_develop               AS if_own_develop
       ,FREQUENCY
       ,area_name
       ,CASE WHEN (t2.FREQUENCY = '首页' or t2.FREQUENCY = '我的') AND t2.AREA_NAME = 'ICON功能区' THEN concat('ICON(',AREA_LOCATION,')')  ELSE t2.AREA_LOCATION END AS area_location
       ,cs_referer                   AS link_referer
       ,wt_es                        AS link_current
       ,CASE WHEN nvl(t2.url,'')='' THEN '--'
             ELSE t2.url END         AS url
       ,issue_id                     AS issue_id
       ,mark_id                      AS mark_id
       ,marke_name                   AS mark_name
       ,marketing_type               AS mark_type
       ,advertype                    AS advertype
       ,CASE WHEN wt_event like '2020bjjtzq_yxqy_%' THEN regexp_extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d',0)
             WHEN wt_event like 'sy_qy_%' THEN concat(regexp_extract(wt_event,'sy_qy_\\d+-\\d+',0),reverse(regexp_extract(reverse(wt_event),'\\d+-\\d+-\\d+',0)))  ELSE wt_event END AS wt_event
       ,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                        AS wt_ti
       ,wt_mc_id                     AS wt_mc_id
       ,wt_ac                        AS wt_ac
       ,nvl(t1.wt_ac_id,t2.wt_ac_id) AS wt_ac_id
       ,goods_id                     AS goods_id
       ,sku_id                       AS sku_id
       ,editor_provname              AS editor_prov_name
       ,marketing_mode               AS mark_mode
       ,If_prov_page                 AS If_prov_page
       ,icon_code                    AS icon_code
       ,login_type                   AS login_type
       ,is_webtends                  AS is_webtends
       ,if_safe_audit                AS if_safe_audit
       ,editor                       AS editor
       ,editor_phone_num             AS editor_phone_num
       ,pro_company                  AS pro_company
       ,template_id                  AS template_id
       ,'adverType误传修正'              AS ANALY_TYPE
       ,CONF_CHAN
       ,POINT_TYPE
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
FROM
(
	SELECT  date_time
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
	FROM ham_jituan.dwd_client_event_di a
	WHERE dt= '${DT}'
	AND trmnl_style = 'IOS'
	AND advertype = '29'
	AND not exists(
	SELECT  marketing_id
	FROM ham_jituan.dim_client_ord_info_d b
	WHERE b.dt='${DT}'
	AND concat(a.advertype, '_', a.mark_id) = b.marketing_id) 
) t1
INNER JOIN
(
	SELECT  SERIAL_ID
	       ,START_TIME
	       ,DOWN_TIME
	       ,STATUS
	       ,ISSUE_RANGE
	       ,IF_OWN_DEVELOP
	       ,AREA_NAME
	       ,AREA_LOCATION
	       ,ISSUE_ID
	       ,MARKE_NAME
	       ,URL
	       ,EDITOR_PROVNAME
	       ,FREQUENCY
	       ,MARKETING_MODE
	       ,IF_PROV_PAGE
	       ,MARKETING_ID
	       ,ICON_CODE
	       ,LOGIN_TYPE
	       ,IS_WEBTENDS
	       ,IF_SAFE_AUDIT
	       ,EDITOR
	       ,EDITOR_PHONE_NUM
	       ,PRO_COMPANY
	       ,TEMPLATE_ID
	       ,MARKETING_TYPE
	       ,WT_AC_ID
	       ,CONF_CHAN
	       ,CASE WHEN (frequency = '首页' or frequency = '我的') AND area_type = 'ICON功能区' THEN 'ICON区域'
	             WHEN frequency = '其他辅助广告位' or (frequency = '首页' AND (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' AND (area_type = '轮播图' or area_type = '营销区域')) THEN '广告位'  ELSE '其他' END AS POINT_TYPE
	       ,AREA_TYPE
	FROM ham_jituan.dim_client_ord_info_d
	WHERE dt='${DT}' 
) t2 --工单表
ON concat('522_', t1.mark_id) = t2.marketing_id
LEFT JOIN
(
	SELECT  prov,name
	FROM ham_jituan.dim_client_city
	group by prov,name 
) t4
ON t1.wt_prov = t4.prov --地市翻译表 
;



-- ios端7.5.0版本数据修复
insert into table ham_jituan.ads_hachi_jzyy_xtb2_51008 
SELECT  wt_mobile                    AS BILL_NO
       ,regexp_replace(wt_cid,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|','') AS IMEI
       ,wt_co_f                      AS cookie
       ,wt_prov                      AS prov_id
       ,nvl(t4.name,'未知')            AS prov_name
       ,wt_city                      AS city_name
       ,channelid                    AS channel_id
       ,c_ip                         AS c_ip
       ,cs_host                      AS cs_host
       ,trmnl_style                  AS trmnl_style
       ,regexp_replace(wt_aav, '\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                       AS app_version
       ,wt_av                        AS sys_version
       ,date_day                     AS create_date
       ,date_time                    AS create_datetime
       ,click_time                   AS click_datetime
       ,serial_id                    AS serial_id
       ,start_time                   AS start_time
       ,down_time                    AS down_time
       ,status                       AS status
       ,issue_range                  AS issue_range
       ,if_own_develop               AS if_own_develop
       ,FREQUENCY
       ,area_name
       ,CASE WHEN (t2.FREQUENCY = '首页' or t2.FREQUENCY = '我的') AND t2.AREA_NAME = 'ICON功能区' THEN concat('ICON(',AREA_LOCATION,')')  ELSE t2.AREA_LOCATION END AS area_location
       ,cs_referer                   AS link_referer
       ,wt_es                        AS link_current
       ,CASE WHEN nvl(t2.url,'')='' THEN '--'
             ELSE t2.url END         AS url
       ,issue_id                     AS issue_id
       ,mark_id                      AS mark_id
       ,marke_name                   AS mark_name
       ,marketing_type               AS mark_type
       ,advertype                    AS advertype
       ,CASE WHEN wt_event like '2020bjjtzq_yxqy_%' THEN regexp_extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d',0)
             WHEN wt_event like 'sy_qy_%' THEN concat(regexp_extract(wt_event,'sy_qy_\\d+-\\d+',0),reverse(regexp_extract(reverse(wt_event),'\\d+-\\d+-\\d+',0)))  ELSE wt_event END AS wt_event
       ,regexp_replace(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')                        AS wt_ti
       ,wt_mc_id                     AS wt_mc_id
       ,wt_ac                        AS wt_ac
       ,nvl(t1.wt_ac_id,t2.wt_ac_id) AS wt_ac_id
       ,goods_id                     AS goods_id
       ,sku_id                       AS sku_id
       ,editor_provname              AS editor_prov_name
       ,marketing_mode               AS mark_mode
       ,If_prov_page                 AS If_prov_page
       ,icon_code                    AS icon_code
       ,login_type                   AS login_type
       ,is_webtends                  AS is_webtends
       ,if_safe_audit                AS if_safe_audit
       ,editor                       AS editor
       ,editor_phone_num             AS editor_phone_num
       ,pro_company                  AS pro_company
       ,template_id                  AS template_id
       ,'IOS端7.5.0版本修正数据'            AS ANALY_TYPE
       ,CONF_CHAN
       ,POINT_TYPE
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
FROM
(
	SELECT  date_time
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
	from ham_jituan.dwd_client_event_di
	WHERE dt= '${DT}'
	AND trmnl_style = 'IOS'
	AND wt_av = 'APP_ios_v7.5.0'
	AND mark_id = '522'
) t1
INNER JOIN
(
	SELECT  SERIAL_ID
	       ,START_TIME
	       ,DOWN_TIME
	       ,STATUS
	       ,ISSUE_RANGE
	       ,IF_OWN_DEVELOP
	       ,AREA_NAME
	       ,AREA_LOCATION
	       ,ISSUE_ID
	       ,MARKE_NAME
	       ,URL
	       ,EDITOR_PROVNAME
	       ,FREQUENCY
	       ,MARKETING_MODE
	       ,IF_PROV_PAGE
	       ,MARKETING_ID
	       ,ICON_CODE
	       ,LOGIN_TYPE
	       ,IS_WEBTENDS
	       ,IF_SAFE_AUDIT
	       ,EDITOR
	       ,EDITOR_PHONE_NUM
	       ,PRO_COMPANY
	       ,TEMPLATE_ID
	       ,MARKETING_TYPE
	       ,WT_AC_ID
	       ,CONF_CHAN
	       ,CASE WHEN (frequency = '首页' or frequency = '我的') AND area_type = 'ICON功能区' THEN 'ICON区域'
	             WHEN frequency = '其他辅助广告位' or (frequency = '首页' AND (area_type = '轮播图' or area_type = '营销区域')) or (frequency = '优惠页' AND (area_type = '轮播图' or area_type = '营销区域')) THEN '广告位'  ELSE '其他' END AS POINT_TYPE
	       ,AREA_TYPE
	from ham_jituan.dim_client_ord_info_d
	WHERE dt ='${DT}'
	AND FREQUENCY = '我的页面（V6.6）'
	AND AREA_NAME = '我的页面icon区域'
) t2 -- 工单表
ON concat('522_', t1.advertype) = t2.marketing_id
LEFT JOIN
(
	SELECT  prov,name
	FROM ham_jituan.dim_client_city -- 地市翻译表
	group by prov,name
) t4
ON t1.wt_prov = t4.prov
;
