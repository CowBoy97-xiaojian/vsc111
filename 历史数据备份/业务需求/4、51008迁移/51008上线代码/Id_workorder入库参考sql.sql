ham_jituan.dim_client_ord_info_d
CREATE TABLE `ham_jituan.dim_client_ord_info_d`(
  `serial_id` string, 
  `start_time` string, 
  `down_time` string, 
  `status` string, 
  `issue_range` string, 
  `if_own_develop` string, 
  `area_name` string, 
  `area_location` string, 
  `issue_id` string, 
  `marke_name` string, 
  `url` string, 
  `editor_provname` string, 
  `frequency` string, 
  `marketing_mode` string, 
  `if_prov_page` string, 
  `marketing_id` string, 
  `icon_code` string, 
  `iop_sub_activity_id` string, 
  `iop_operation_id` string, 
  `promotional_content` string, 
  `contacts_id` string, 
  `page_id` string, 
  `login_type` string, 
  `is_webtends` string, 
  `if_safe_audit` string, 
  `editor` string, 
  `editor_phone_num` string, 
  `pro_company` string, 
  `template_id` string, 
  `marketing_type` string, 
  `wt_ac_id` string, 
  `conf_chan` string, 
  `area_type` string)
COMMENT 'created by dacp create table function'
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham_jituan.db/dim_client_ord_info_d'
TBLPROPERTIES (
  'last_modified_by'='udbac', 
  'last_modified_time'='1638170840', 
  'transient_lastDdlTime'='1638170840')

ham_jituan.dim_client_work_order_d
CREATE EXTERNAL TABLE `ham_jituan.dim_client_work_order_d`(
  `record_num` string COMMENT 'from deserializer', 
  `serial_id` string COMMENT 'from deserializer', 
  `start_time` string COMMENT 'from deserializer', 
  `down_time` string COMMENT 'from deserializer', 
  `status` string COMMENT 'from deserializer', 
  `issue_range` string COMMENT 'from deserializer', 
  `if_own_develop` string COMMENT 'from deserializer', 
  `area_name` string COMMENT 'from deserializer', 
  `area_location` string COMMENT 'from deserializer', 
  `issue_id` string COMMENT 'from deserializer', 
  `marke_name` string COMMENT 'from deserializer', 
  `url` string COMMENT 'from deserializer', 
  `editor_provname` string COMMENT 'from deserializer', 
  `frequency` string COMMENT 'from deserializer', 
  `marketing_mode` string COMMENT 'from deserializer', 
  `if_prov_page` string COMMENT 'from deserializer', 
  `marketing_id` string COMMENT 'from deserializer', 
  `icon_code` string COMMENT 'from deserializer', 
  `iop_sub_activity_id` string COMMENT 'from deserializer', 
  `iop_operation_id` string COMMENT 'from deserializer', 
  `promotional_content` string COMMENT 'from deserializer', 
  `contacts_id` string COMMENT 'from deserializer', 
  `page_id` string COMMENT 'from deserializer', 
  `login_type` string COMMENT 'from deserializer', 
  `is_webtends` string COMMENT 'from deserializer', 
  `if_safe_audit` string COMMENT 'from deserializer', 
  `editor` string COMMENT 'from deserializer', 
  `editor_phone_num` string COMMENT 'from deserializer', 
  `pro_company` string COMMENT 'from deserializer', 
  `template_id` string COMMENT 'from deserializer', 
  `marketing_type` string COMMENT 'from deserializer', 
  `wt_ac_id` string COMMENT 'from deserializer', 
  `sub_title` string COMMENT 'from deserializer', 
  `postage_type` string COMMENT 'from deserializer', 
  `tariff_code` string COMMENT 'from deserializer', 
  `boss_id` string COMMENT 'from deserializer')
COMMENT 'created by dacp create table function'
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u20AC\u20AC') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham_jituan.db/dim_client_work_order_d'
TBLPROPERTIES (
  'transient_lastDdlTime'='1632290944')



ALTER TABLE ham_jituan.dim_client_work_order_d DROP partition(dt='${DT}');
ALTER TABLE ham_jituan.dim_client_work_order_d ADD partition(dt='${DT}') LOCATION '/data/ld_workorder/${DT}';

insert overwrite table ham_jituan.dim_client_ord_info_d partition(dt = '${DT}')
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
       ,IOP_SUB_ACTIVITY_ID        
       ,IOP_OPERATION_ID           
       ,PROMOTIONAL_CONTENT        
       ,CONTACTS_ID                
       ,PAGE_ID 
       ,LOGIN_TYPE      
       ,IS_WEBTENDS     
       ,IF_SAFE_AUDIT   
       ,EDITOR          
       ,EDITOR_PHONE_NUM
       ,PRO_COMPANY     
       ,TEMPLATE_ID     
       ,MARKETING_TYPE  
       ,WT_AC_ID     
       ,if(editor_provname='全国','一级渠道','省公司配置') as conf_chan
       ,case when area_name like 'ICON功能区%' or area_name='首页—模板1' or frequency='通话类功能页' then 'ICON功能区'
when frequency='我的' and icon_code rlike '^[a-zA-Z]{2}[0-9]{5,10}$' then 'ICON功能区' 
else area_name end as area_type 
from ham_jituan.dim_client_work_order_d
where dt='${DT}'  
and((frequency<>'底部导航栏'
     and (status=1 or status='--' or (status=0 and frequency='新生活TAB') or (status=4 and down_time >='${DT} 00:10:00' and substr(down_time,1,10)<= date_add('${DT}',1)))
     and start_time <='${DT} 23:59:59'  
     and instr(area_name,'测试') <= 0 
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
       ,IOP_SUB_ACTIVITY_ID        
       ,IOP_OPERATION_ID           
       ,PROMOTIONAL_CONTENT        
       ,CONTACTS_ID                
       ,PAGE_ID
       ,LOGIN_TYPE      
       ,IS_WEBTENDS     
       ,IF_SAFE_AUDIT   
       ,EDITOR          
       ,EDITOR_PHONE_NUM
       ,PRO_COMPANY     
       ,TEMPLATE_ID     
       ,MARKETING_TYPE  
       ,WT_AC_ID;