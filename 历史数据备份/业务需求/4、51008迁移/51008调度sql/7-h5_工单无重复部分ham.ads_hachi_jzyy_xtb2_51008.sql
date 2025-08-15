--没有数据
-- 7、h5_工单无重复部分ham.ads_hachi_jzyy_xtb2_51008
insert into table ham.ads_hachi_jzyy_xtb2_51008_all
select        wt_mobile          as bill_no
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
             ,case when ifnull(t2.url,'')='' then '--' 
                   else t2.url end as url
             ,issue_id           as issue_id
             ,mark_id            as mark_id
             ,marke_name         as mark_name
             ,marketing_type     as mark_type
             ,advertype          as advertype
             ,case when wt_event like '2020bjjtzq_yxqy_%' then extract(wt_event,'2020bjjtzq_yxqy_\\d+-\\d')
                   when wt_event like 'sy_qy_%' then concat(extract(wt_event,'sy_qy_\\d+-\\d+'),reverse(extract(reverse(wt_event),'\\d+-\\d+-\\d+'))) 
                   else wt_event end as wt_event
             ,replaceRegexpAll(wt_ti,'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', '')              as wt_ti
             ,wt_mc_id           as wt_mc_id
             ,wt_ac              as wt_ac
             ,ifnull(t1.wt_ac_id,t2.wt_ac_id)    as wt_ac_id
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
,dt
,hour
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
     where dt= '2023-04-07'
     and trmnl_style in ('H5','b95440ef47ec01fc')) t1
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
           from ham.tmp_c_cth_cm_log_d_02_all) t2 --工单表
on t1.mark_id = t2.marketing_id  
inner join ham.dim_client_h5_all t3 --码表
on 1 = 1            
left join (select prov,name
           from ham.dim_client_city_all 
           group by prov,name) t4 
on t1.wt_prov = t4.prov                --地市翻译表                      
where locate(t3.event,t1.wt_event) > 0 
and t3.pindao = t2.frequency
;