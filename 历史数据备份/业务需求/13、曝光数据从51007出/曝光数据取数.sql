select 
area_id,
count(1) as pv,
count(distinct wt_co_f) as uv,
count(distinct wt_mobile) as user
from webtrends.event_hi_client_all
where et='imp' and imp_type ='once'
and dt = '2023-05-27'
group by area_id
FORMAT CSVWithNames;


wt_event

select 
area_id,
wt_event,
count(1) as pv,
count(distinct wt_co_f) as uv,
count(distinct wt_mobile) as user
from webtrends.event_hi_client_all
where et='imp' and imp_type ='once'
and dt = '2023-05-23'
group by area_id,wt_event
;

select 
area_id,
wt_event,
count(1) as pv,
count(distinct wt_co_f) as uv,
count(distinct wt_mobile) as user
from webtrends.event_hi_client_all
where et='imp' and imp_type ='once'
and dt = '2023-05-23'
group by area_id,wt_event
;



曝光  

区域    p码    曝光数据   点击数据   

--区域   p码
select 
area_id,
wt_event
from webtrends.event_hi_client_all
where et='imp' and imp_type ='once'
and dt = '2023-05-23'
group by area_id,wt_event;




select
wt_event,
count(1) as pv,
count(distinct wt_co_f) as uv,
count(distinct wt_mobile) as user
from (select 
        area_id as zarea,
        wt_event as zevent,
        et
    from webtrends.event_hi_client_all
    where et='imp' and imp_type ='once'
    and dt = '2023-05-23'
    group by area_id,wt_event,et)
where et='clk'
and wt_event = zevent
group by wt_event
;





select 
area_id,
wt_event,
attributes,
count(1) as pv,
count(distinct wt_co_f) as uv,
count(distinct wt_mobile) as user
from webtrends.event_hi_client_all
where et='imp' and imp_type ='once'
and dt = '2023-05-22'
group by area_id,wt_event,attributes
;




select
wt_event,
count(1) as pv,
count(distinct wt_co_f) as uv,
count(distinct wt_mobile) as user
from webtrends.event_hi_client_all
where et='clk' 
and wt_event in ('P00000043178',
'P00000043195',
'P00000043377',
'P00000043372',
'P00000043152',
'P00000043215',
'P00000043272',
'P00000043317',
'P00000043353',
'P00000043173',
'P00000043319',
'P00000043297',
'P00000043371',
'P00000043258',
'P00000043352',
'P00000043273',
'P00000043296',
'P00000043191',
'P00000043276',
'P00000043301',
'P00000043131',
'P00000043157',
'P00000043248',
'P00000043252',
'P00000043243',
'P00000043159',
'P00000043331',
'P00000043237',
'P00000043257',
'P00000043198',
'P00000043156',
'P00000043299',
'P00000043192',
'P00000043154',
'P00000043219',
'P00000043314',
'P00000043171',
'P00000043220',
'P00000043311',
'P00000043259',
'P00000043155',
'P00000043375',
'P00000043193',
'P00000043335',
'P00000043292',
'P00000043213',
'P00000043177',
'P00000043316',
'P00000043300',
'P00000043354',
'P00000043172',
'P00000043238',
'P00000043373',
'P00000043197',
'P00000043199',
'P00000043212',
'P00000043151',
'P00000043134',
'P00000043196',
'P00000043333',
'P00000043234',
'P00000043291',
'P00000043254',
'P00000043357',
'P00000043179',
'P00000043133',
'P00000043132',
'P00000043318',
'P00000043334',
'P00000043298',
'P00000043336',
'P00000043174',
'P00000043175',
'P00000043277',
'P00000043153',
'P00000043176',
'P00000043274',
'P00000043216')
and dt = '2023-05-23'
group by wt_event
;

select
et,
wt_event,
attributes
from webtrends.event_hi_client_all
where et='clk' 
and wt_event like '%P000000%'
and dt = '2023-05-20'
limit 5
;


P00000043157

select 
area_id,
wt_event,
count(1) as pv,
count(distinct wt_co_f) as uv,
count(distinct wt_mobile) as user
from webtrends.event_hi_client_all
where et='clk'
and att wt_event in ('P00000043157','P00000043154','P00000043218','P00000043172','P00000043196','P00000043132','P00000043176') 
--and dt = '2023-05-19'
group by area_id,wt_event
limit 5
;

select 
et,wt_event
from ham_jituan.dwd_client_event_di
where et='imp'
and dt='2023-04-27' limit 35;



select 
wt_event,
count(1) as pv,
count(distinct wt_co_f) as uv,
count(distinct wt_mobile) as user
from ham_jituan.dwd_client_event_di
where et='clk'
and wt_event in ('P00000043157','P00000043154','P00000043218','P00000043172','P00000043196','P00000043132','P00000043176') 
--and dt = '2023-05-11'
group by wt_event
limit 5
;


dwd_client_event_di


select 
*
from webtrends.event_hi_client_all
where wt_event ='P00000043154'
and dt = '2023-05-19'
limit 5 FORMAT CSVWithNames
;

{"$data_source_id":"b95440ef47ec01fc","WT_loginProvince":"851","$path":"/cmcc-app-gray/homePlusNew/index.html","$index":"0","$os":"web","$sdk_version":"3.8.3","$platform":"web","$ip":"39.174.91.203","area_id":"INDEX_JT_RMHD","type":"once","$device_orientation":"PORTRAIT","$user_agent":"Mozilla/5.0 (Linux; Android 11; V1916A Build/RP1A.200720.012; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/83.0.4103.106 Mobile Safari/537.36 leadeon/8.9.0.1/CMCCIT","userId":"","$language":"zh-CN","WT_prov":"250","WT_city":"025","WT_clientID":"50R+35D/hHVCeAVEMJeCnHSuXMaikRiUfk9uFjVsHyPFv//lq29suaqpO6iUPHGw1vJGkmdql1LZ0/djy47giWtdtnhW4AOTJt/Wb1iJRfH3DLkX/oKzlUd5eR+GQCqNkkP4Mbp68xM=","$title":"中国移动手机营业厅首页PLUS","WT_userBrand":"09","WT_loginCity":"0851","$client_version":"1.0.0","$domain":"testh.app.coc.10086.cn","WT_cid":"50R+35D/hHVCeAVEMJeCnHSuXMaikRiUfk9uFjVsHyPFv//lq29suaqpO6iUPHGw1vJGkmdql1LZ0/djy47giWtdtnhW4AOTJt/Wb1iJRfH3DLkX/oKzlUd5eR+GQCqNkkP4Mbp68xM="}

{"$data_source_id":"b95440ef47ec01fc","WT_loginProvince":"250","$path":"/cmcc-app-gray/homePlusNew/index.html","$index":"0","$os":"web","$sdk_version":"3.8.3","$platform":"web","$ip":"219.144.235.176","area_id":"INDEX_JT_RMHD","type":"once","$device_orientation":"PORTRAIT","$user_agent":"Mozilla/5.0 (Linux; Android 10; V2002A Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/83.0.4103.106 Mobile Safari/537.36 leadeon/8.9.0.1/CMCCIT","userId":"","$language":"zh-CN","WT_prov":"250","WT_city":"0516","WT_clientID":"EqNR2EvpvVLWYGGSxhAnUpgca6zOMeFUNmBLnW3ZZYw97OiRQxT1selUfSfXDozQU2H8wHqWPvObxsBn5eP24bOIGV4a29xY5CxhxH87ej/i1Ke+fvLsgnNCRMB4dq+wS6WGjJ6glq4=","$title":"中国移动手机营业厅首页PLUS","WT_userBrand":"01","WT_loginCity":"0516","$client_version":"1.0.0","$domain":"testh.app.coc.10086.cn","WT_cid":"EqNR2EvpvVLWYGGSxhAnUpgca6zOMeFUNmBLnW3ZZYw97OiRQxT1selUfSfXDozQU2H8wHqWPvObxsBn5eP24bOIGV4a29xY5CxhxH87ej/i1Ke+fvLsgnNCRMB4dq+wS6WGjJ6glq4="}


select attributes 
from olap.event_all 
where toDate(event_time) = '2023-05-19' 
and attributes['area_id'] = 'INDEX_JT_RMHD'
and attributes['area_id']
limit 5;




select 
*
from webtrends.event_hi_client_all
where wt_event ='P00000043157'
and dt = '2023-05-19'
limit 5 FORMAT CSVWithNames
;


select 
attributes 
from olap.event_all 
where toDate(event_time) = '2023-04-27' 
and attributes['area_id'] is not null
and et
limit 5;

select 
attributes 
from olap.event_all 
where toDate(event_time) = '2023-04-27' 
and attributes['area_id'] is not null
and wt_event = 'P00000043374'
limit 5;
P00000043374
P00000043372



50条曝光数据
SELECT
clientid,
area_id,
imp_type,
et,
envname,
wt_event,
mark_id,
wt_mobile,
wt_es
from webtrends.event_hi_client_all
where et='imp' and imp_type ='once'
and dt = '2023-05-27'
limit 50 FORMAT CSVWithNames;
