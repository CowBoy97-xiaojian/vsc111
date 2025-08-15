insert into table dim_client_ord_info_d_all(serial_id,start_time,down_time,status,issue_range,if_own_develop,area_name,area_location,issue_id,marke_name,url,editor_provname,frequency,marketing_mode,if_prov_page,marketing_id,icon_code,iop_sub_activity_id,iop_operation_id,promotional_content,contacts_id,page_id,login_type,is_webtends,if_safe_audit,editor,editor_phone_num,pro_company,template_id,marketing_type,wt_ac_id,conf_chan,area_type,dt)
select serial_id                  
       ,start_time                 
       ,down_time                  
       ,status                     
       ,issue_range                
       ,if_own_develop             
       ,case when (area_name = '首页—模板1' or area_name like 'icon区%') then 'ICON功能区' else area_name end as area_name                  
       ,area_location              
       ,issue_id                   
       ,marke_name                 
       ,url                        
       ,editor_provname            
       ,case when frequency like '首页2.0%' then '首页2.0'
	     when frequency like '首页%' then '首页'
             when frequency like '%新生活TAB%' then '新生活'
             when frequency like '%个性化业务--异网专区%' then '个性业务--异网专区'
             when frequency like '%个性化业务--新家庭专区%' then '个性业务--新家庭专区'
             when frequency like '%流量专区%' then '个性业务-流量专区'
             when frequency like '%余额查询%' then '通话类功能页'
             else frequency end as frequency                 
       ,marketing_mode             
       ,if_prov_page               
       ,marketing_id               
       ,icon_code                  
       ,iop_sub_activity_id        
       ,iop_operation_id           
       ,promotional_content        
       ,contacts_id                
       ,page_id 
       ,login_type      
       ,is_webtends     
       ,if_safe_audit   
       ,editor          
       ,editor_phone_num
       ,pro_company     
       ,template_id     
       ,marketing_type  
       ,wt_ac_id     
       ,if(editor_provname='全国','一级渠道','省公司配置') as conf_chan
       ,case when area_name like 'ICON功能区%' or area_name='首页—模板1' or frequency='通话类功能页' then 'ICON功能区'
when frequency='我的' and match(icon_code,'^[a-zA-Z]{2}[0-9]{5,10}$') then 'ICON功能区' 
else area_name end as area_type,
       dt
from ham.dim_client_work_order_d_all
where dt='${DT}'  
and ((frequency<>'底部导航栏'
     and (status='1' or status='--' or (status='0' and frequency='新生活TAB') or (status='4' and down_time >='${DT} 00:10:00' and toDate(substr(down_time,1,10)) <= addDays(toDate('${DT}'), 1)))
     and start_time <='${DT} 23:59:59'  
     and match(area_name,'测试') <= 0 
     and trim(area_name)<>'其他辅助广告位' 
     and area_name<>'首页icon'
     and frequency is not null) 
    or frequency='底部导航栏')
group by serial_id                  
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
       ,iop_sub_activity_id        
       ,iop_operation_id           
       ,promotional_content        
       ,contacts_id                
       ,page_id
       ,login_type      
       ,is_webtends     
       ,if_safe_audit   
       ,editor          
       ,editor_phone_num
       ,pro_company     
       ,template_id     
       ,marketing_type 
       ,wt_ac_id
       ,dt;
