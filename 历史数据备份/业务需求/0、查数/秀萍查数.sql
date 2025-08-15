一
select 
area_id,
count(1) as pv,
count(distinct wt_co_f) as uv,
count(distinct wt_mobile) as user
from webtrends.event_hi_client_all
where et='imp' and imp_type ='once'
and dt = '2023-05-26'
group by area_id
FORMAT CSVWithNames;


二
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
and dt = '2023-05-26'
limit 50 FORMAT CSVWithNames;


三

SELECT 
T1.area_id   
,T1.wt_event 
,T1.envname
,T1.pv       AS  BG_pv 
,T1.uv       AS  BG_uv  
,T1.user     AS  BG_user
,T2.pv       AS  DG_pv
,T2.pv/T1.pv AS  pv_RATION   
,T2.uv       AS  DG_uv
,T2.uv/T1.uv AS  uv_RATION  
,T2.user  AS  DG_user
,T2.user/T1.user  AS  user_RATION
FROM 
(SELECT
    area_id,
    wt_event,
    envname,
    count(1) AS pv,
    countDistinct(wt_co_f) AS uv,
    countDistinct(wt_mobile) AS user
FROM webtrends.event_hi_client_all
WHERE (et = 'imp') AND (imp_type = 'once') AND (dt = '2023-05-26')
GROUP BY
    area_id,
    wt_event,
    envname
)T1
LEFT JOIN 
(
SELECT
    wt_event,
    count(1) AS pv,
    countDistinct(wt_co_f) AS uv,
    countDistinct(wt_mobile) AS user
FROM webtrends.event_hi_client_all
WHERE (et = 'clk') AND (dt = '2023-05-26')
GROUP BY wt_event
)T2
ON T1.wt_event = T2.wt_event
FORMAT CSVWithNames;



4
SELECT
    count(1) AS pv,
    countDistinct(wt_co_f) AS uv,
    countDistinct(wt_mobile) AS user
FROM webtrends.event_hi_client_all
WHERE (dt between '2023-05-26' and '2023-05-31') and  
(et = 'pageview') AND （wt_event='H5pageshow' ）AND (wt_es like '%homePlusNew%')