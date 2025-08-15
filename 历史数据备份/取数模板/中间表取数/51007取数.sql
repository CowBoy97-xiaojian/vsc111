wt渠道号
('dcs47gbrugonmg3u1x8njabg1_2p4f','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs4311adgonmgj23shb4oqyy_5q5k')



--按照渠道对比数据
SELECT
trmnl_style,
count(1)
FROM webtrends.event_hi_client_update_all
WHERE (dt = '2023-06-16') AND (hour = '10') AND (trmnl_style IN ('H5', 'ANDROID', 'IOS', 'MPX_App')) 
GROUP BY trmnl_style
FORMAT CSVWithNames;


SELECT
data_source_id,
count(1)
FROM olap.event_all
WHERE (toDate(event_time) = '2023-06-23') AND (toHour(event_time) = '10') AND (data_source_id IN ('dcs47gbrugonmg3u1x8njabg1_2p4f','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs4311adgonmgj23shb4oqyy_5q5k'))
GROUP BY data_source_id
FORMAT CSVWithNames;


--按照小时对比数据

SELECT
hour,
count(1) as sum
FROM webtrends.event_hi_client_update_all 
WHERE dt = '2023-06-16' 
AND  dcsid IN ('dcs47gbrugonmg3u1x8njabg1_2p4f','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs4311adgonmgj23shb4oqyy_5q5k')
group by hour
order by hour
FORMAT CSVWithNames
;


SELECT
toHour(event_time) as hour,
count(1) as sum
FROM olap.event_all
WHERE toDate(event_time) = '2023-06-16' 
AND  data_source_id IN ('dcs47gbrugonmg3u1x8njabg1_2p4f','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs4311adgonmgj23shb4oqyy_5q5k')
group by hour
order by hour
FORMAT CSVWithNames
;



SELECT
hour,
count(1)
FROM webtrends.event_hi_client_update_all 
WHERE dt = '2023-06-16' 
AND  dcsid IN ('dcs47gbrugonmg3u1x8njabg1_2p4f','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs4311adgonmgj23shb4oqyy_5q5k')
AND hour in ('10','13')
group by hour
order by hour
FORMAT CSVWithNames
;

SELECT
dt,
count(1)
FROM webtrends.event_hi_client_update_all 
WHERE dt in ('2023-06-16','2023-06-17')
group by dt
FORMAT CSVWithNames
;


select attributes from olap.event_all where toDate(event_time) = '2023-06-26' and attributes['WT_event']= '634_1574' and data_source_id in ('dcs47gbrugonmg3u1x8njabg1_2p4f','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs4311adgonmgj23shb4oqyy_5q5k') limit 10 ;



select attributes from olap.event_all where toDate(event_time) = '2023-06-26' and attributes['markId']= '1574' and data_source_id in ('dcs47gbrugonmg3u1x8njabg1_2p4f','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs4311adgonmgj23shb4oqyy_5q5k') limit 10 ;