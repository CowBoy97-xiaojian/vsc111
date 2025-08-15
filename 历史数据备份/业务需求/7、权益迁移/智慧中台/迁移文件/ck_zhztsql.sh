
#! /bin/bash

DT=$1
hour=$2

#1、删除分区--重跑
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="alter table ham.ads_zhzt_amt_di_local on cluster cluster_gio_with_shard drop partition '2023-04-07'"

#2、导入数据
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="
insert into ham.ads_zhzt_amt_di_all
SELECT  regexp_replace('2023-04-07','-','')
       ,'B99999999999'
       ,'A202900002'
       ,'S10291200038'
       ,m.name
       ,m.code
       ,CASE WHEN s.prov = '280' THEN '全息作战体系'
             WHEN s.prov = '100' THEN '插码数据能力'
             WHEN s.prov = '551' THEN '安徽移动插码系统'
             WHEN s.prov = '771' THEN '中国移动广西APP'
             WHEN s.prov = '851' THEN '通用能力平台'
             WHEN s.prov = '250' THEN '和洞察对外大数据服务平台'  ELSE '' END
       ,CASE WHEN s.prov = '280' THEN '28001007'
             WHEN s.prov = '100' THEN '10001103'
             WHEN s.prov = '551' THEN '55101079'
             WHEN s.prov = '771' THEN '77101040'
             WHEN s.prov = '851' THEN '85101022'
             WHEN s.prov = '250' THEN '25002216'  ELSE '' END
       ,CASE WHEN s.prov = '280' THEN '四川中台能力运营中心'
             WHEN s.prov = '100' THEN '北京中台能力运营中心'
             WHEN s.prov = '551' THEN '安徽中台能力运营中心'
             WHEN s.prov = '771' THEN '广西中台能力运营中心'
             WHEN s.prov = '851' THEN '贵州中台能力运营中心'
             WHEN s.prov = '250' THEN '江苏中台能力运营中心'  ELSE '' END
       ,CASE WHEN s.prov = '280' THEN '28001'
             WHEN s.prov = '100' THEN '10001'
             WHEN s.prov = '551' THEN '55101'
             WHEN s.prov = '771' THEN '77101'
             WHEN s.prov = '851' THEN '85101'
             WHEN s.prov = '250' THEN '25002'  ELSE '' END
       ,''
       ,CASE WHEN s.prov = '280' THEN 'AOR20221117008801'
             WHEN s.prov = '100' THEN 'AOR20221129001701'
             WHEN s.prov = '551' THEN 'AOR20221129002501'
             WHEN s.prov = '771' THEN 'AOR20221130010001'
             WHEN s.prov = '851' THEN 'AOR20221201000101'
             WHEN s.prov = '250' THEN 'AOR20221212011101'  ELSE '' END
       ,concat('{"ZLG":{"xtcll":',toString(s.pv),'}}')
       ,'{"ZLG":{"ZSYY":{"xtcll":0},"FZSYY":{"xtcll":0}}}'
       ,'2023-04-07'
FROM
(
     SELECT  prov
            ,COUNT(1) AS pv
     FROM webtrends.event_hi_dcslog_all
     WHERE dt = '2023-04-07'
     GROUP BY  prov
)s
INNER JOIN ham.dim_capacity_user_code m
ON s.prov = m.code
;"



clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="
insert into table ham.ads_zhzt_amt_di_all
SELECT  regexp_replace('2023-04-07','-','')
       ,'B99999999999'
       ,'A202900002'
       ,'S10291200040'
       ,m.name
       ,m.code
       ,CASE WHEN s.prov = '280' THEN '全息作战体系'
             WHEN s.prov = '100' THEN '插码数据能力'
             WHEN s.prov = '551' THEN '安徽移动插码系统'
             WHEN s.prov = '771' THEN '中国移动广西APP'
             WHEN s.prov = '851' THEN '通用能力平台'
             WHEN s.prov = '250' THEN '和洞察对外大数据服务平台'  ELSE '' END
       ,CASE WHEN s.prov = '280' THEN '28001007'
             WHEN s.prov = '100' THEN '10001103'
             WHEN s.prov = '551' THEN '55101079'
             WHEN s.prov = '771' THEN '77101040'
             WHEN s.prov = '851' THEN '85101022'
             WHEN s.prov = '250' THEN '25002216'  ELSE '' END
       ,CASE WHEN s.prov = '280' THEN '四川中台能力运营中心'
             WHEN s.prov = '100' THEN '北京中台能力运营中心'
             WHEN s.prov = '551' THEN '安徽中台能力运营中心'
             WHEN s.prov = '771' THEN '广西中台能力运营中心'
             WHEN s.prov = '851' THEN '贵州中台能力运营中心'
             WHEN s.prov = '250' THEN '江苏中台能力运营中心'  ELSE '' END
       ,CASE WHEN s.prov = '280' THEN '28001'
             WHEN s.prov = '100' THEN '10001'
             WHEN s.prov = '551' THEN '55101'
             WHEN s.prov = '771' THEN '77101'
             WHEN s.prov = '851' THEN '85101'
             WHEN s.prov = '250' THEN '25002'  ELSE '' END
       ,''
       ,CASE WHEN s.prov = '280' THEN 'AOR20221117008801'
             WHEN s.prov = '100' THEN 'AOR20221129001701'
             WHEN s.prov = '551' THEN 'AOR20221129002501'
             WHEN s.prov = '771' THEN 'AOR20221130010001'
             WHEN s.prov = '851' THEN 'AOR20221201000101'
             WHEN s.prov = '250' THEN 'AOR20221212011101'  ELSE '' END
       ,concat('{"ZLG":{"xtcll":',toString(s.pv),'}}')
       ,'{"ZLG":{"ZSYY":{"xtcll":0},"FZSYY":{"xtcll":0}}}'
       ,'2023-04-07'
FROM
(
     SELECT  prov
            ,COUNT(1) AS pv
     FROM webtrends.event_hi_dcslog_all
     WHERE dt = '2023-04-07'
     GROUP BY  prov
)s
INNER JOIN ham.dim_capacity_user_code m
ON s.prov = m.code
;
"


clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="
insert into table ham.ads_zhzt_amt_di_all
SELECT  regexp_replace('2023-04-07','-','')
       ,'B99999999999'
       ,'A202900002'
       ,'S10291200039'
       ,m.name
       ,m.code
       ,CASE WHEN s.prov = '280' THEN '全息作战体系'
             WHEN s.prov = '100' THEN '插码数据能力'
             WHEN s.prov = '551' THEN '安徽移动插码系统'
             WHEN s.prov = '771' THEN '中国移动广西APP'
             WHEN s.prov = '851' THEN '通用能力平台'
             WHEN s.prov = '250' THEN '和洞察对外大数据服务平台'  ELSE '' END
       ,CASE WHEN s.prov = '280' THEN '28001007'
             WHEN s.prov = '100' THEN '10001103'
             WHEN s.prov = '551' THEN '55101079'
             WHEN s.prov = '771' THEN '77101040'
             WHEN s.prov = '851' THEN '85101022'
             WHEN s.prov = '250' THEN '25002216'  ELSE '' END
       ,CASE WHEN s.prov = '280' THEN '四川中台能力运营中心'
             WHEN s.prov = '100' THEN '北京中台能力运营中心'
             WHEN s.prov = '551' THEN '安徽中台能力运营中心'
             WHEN s.prov = '771' THEN '广西中台能力运营中心'
             WHEN s.prov = '851' THEN '贵州中台能力运营中心'
             WHEN s.prov = '250' THEN '江苏中台能力运营中心'  ELSE '' END
       ,CASE WHEN s.prov = '280' THEN '28001'
             WHEN s.prov = '100' THEN '10001'
             WHEN s.prov = '551' THEN '55101'
             WHEN s.prov = '771' THEN '77101'
             WHEN s.prov = '851' THEN '85101'
             WHEN s.prov = '250' THEN '25002'  ELSE '' END
       ,''
       ,CASE WHEN s.prov = '280' THEN 'AOR20221117008801'
             WHEN s.prov = '100' THEN 'AOR20221129001701'
             WHEN s.prov = '551' THEN 'AOR20221129002501'
             WHEN s.prov = '771' THEN 'AOR20221130010001'
             WHEN s.prov = '851' THEN 'AOR20221201000101'
             WHEN s.prov = '250' THEN 'AOR20221212011101'  ELSE '' END
       ,concat('{"ZLG":{"xtcll":',toString(s.pv),'}}')
       ,'{"ZLG":{"ZSYY":{"xtcll":0},"FZSYY":{"xtcll":0}}}'
       ,'2023-04-07'
FROM
(
     SELECT  prov
            ,COUNT(1) AS pv
     FROM webtrends.event_hi_dcslog_all
     WHERE dt = '2023-04-07'
     and prov in ('280','100','551','771','851','250')
     GROUP BY  prov
)s
INNER JOIN ham.dim_capacity_user_code m
ON s.prov = m.code
;
"
