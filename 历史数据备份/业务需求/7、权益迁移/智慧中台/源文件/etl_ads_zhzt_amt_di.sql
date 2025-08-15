set hive.default.fileformat = TextFile;
set hive.execution.engine = mr;
set hive.vectorized.execution.enabled  = false;

alter table ham_jituan.ads_zhzt_amt_di drop partition (dt='${DT}');
insert into table ham_jituan.ads_zhzt_amt_di partition (dt='${DT}')
SELECT  regexp_replace('${DT}','-','')
       ,'B99999999999'
       ,'A202900002'
       ,'S10291200038'
       ,m.name
       ,m.code
       ,CASE WHEN s.wt_prov = '280' THEN '全息作战体系'
             WHEN s.wt_prov = '100' THEN '插码数据能力'
             WHEN s.wt_prov = '551' THEN '安徽移动插码系统'
             WHEN s.wt_prov = '771' THEN '中国移动广西APP'
             WHEN s.wt_prov = '851' THEN '通用能力平台'
             WHEN s.wt_prov = '250' THEN '和洞察对外大数据服务平台'  ELSE '' END
       ,CASE WHEN s.wt_prov = '280' THEN '28001007'
             WHEN s.wt_prov = '100' THEN '10001103'
             WHEN s.wt_prov = '551' THEN '55101079'
             WHEN s.wt_prov = '771' THEN '77101040'
             WHEN s.wt_prov = '851' THEN '85101022'
             WHEN s.wt_prov = '250' THEN '25002216'  ELSE '' END
       ,CASE WHEN s.wt_prov = '280' THEN '四川中台能力运营中心'
             WHEN s.wt_prov = '100' THEN '北京中台能力运营中心'
             WHEN s.wt_prov = '551' THEN '安徽中台能力运营中心'
             WHEN s.wt_prov = '771' THEN '广西中台能力运营中心'
             WHEN s.wt_prov = '851' THEN '贵州中台能力运营中心'
             WHEN s.wt_prov = '250' THEN '江苏中台能力运营中心'  ELSE '' END
       ,CASE WHEN s.wt_prov = '280' THEN '28001'
             WHEN s.wt_prov = '100' THEN '10001'
             WHEN s.wt_prov = '551' THEN '55101'
             WHEN s.wt_prov = '771' THEN '77101'
             WHEN s.wt_prov = '851' THEN '85101'
             WHEN s.wt_prov = '250' THEN '25002'  ELSE '' END
       ,''
       ,CASE WHEN s.wt_prov = '280' THEN 'AOR20221117008801'
             WHEN s.wt_prov = '100' THEN 'AOR20221129001701'
             WHEN s.wt_prov = '551' THEN 'AOR20221129002501'
             WHEN s.wt_prov = '771' THEN 'AOR20221130010001'
             WHEN s.wt_prov = '851' THEN 'AOR20221201000101'
             WHEN s.wt_prov = '250' THEN 'AOR20221212011101'  ELSE '' END
       ,concat('{"ZLG":{"xtcll":',s.pv,'}}')
       ,'{"ZLG":{"ZSYY":{"xtcll":0},"FZSYY":{"xtcll":0}}}'
FROM
(
     SELECT  wt_prov
            ,COUNT(1) AS pv
     FROM ham_jituan.dwd_client_event_di
     WHERE dt = '${DT}'
     GROUP BY  wt_prov
)s
INNER JOIN ham.dim_capacity_user_code m
ON s.wt_prov = m.code
;
insert into table ham_jituan.ads_zhzt_amt_di partition (dt='${DT}')
SELECT  regexp_replace('${DT}','-','')
       ,'B99999999999'
       ,'A202900002'
       ,'S10291200040'
       ,m.name
       ,m.code
       ,CASE WHEN s.wt_prov = '280' THEN '全息作战体系'
             WHEN s.wt_prov = '100' THEN '插码数据能力'
             WHEN s.wt_prov = '551' THEN '安徽移动插码系统'
             WHEN s.wt_prov = '771' THEN '中国移动广西APP'
             WHEN s.wt_prov = '851' THEN '通用能力平台'
             WHEN s.wt_prov = '250' THEN '和洞察对外大数据服务平台'  ELSE '' END
       ,CASE WHEN s.wt_prov = '280' THEN '28001007'
             WHEN s.wt_prov = '100' THEN '10001103'
             WHEN s.wt_prov = '551' THEN '55101079'
             WHEN s.wt_prov = '771' THEN '77101040'
             WHEN s.wt_prov = '851' THEN '85101022'
             WHEN s.wt_prov = '250' THEN '25002216'  ELSE '' END
       ,CASE WHEN s.wt_prov = '280' THEN '四川中台能力运营中心'
             WHEN s.wt_prov = '100' THEN '北京中台能力运营中心'
             WHEN s.wt_prov = '551' THEN '安徽中台能力运营中心'
             WHEN s.wt_prov = '771' THEN '广西中台能力运营中心'
             WHEN s.wt_prov = '851' THEN '贵州中台能力运营中心'
             WHEN s.wt_prov = '250' THEN '江苏中台能力运营中心'  ELSE '' END
       ,CASE WHEN s.wt_prov = '280' THEN '28001'
             WHEN s.wt_prov = '100' THEN '10001'
             WHEN s.wt_prov = '551' THEN '55101'
             WHEN s.wt_prov = '771' THEN '77101'
             WHEN s.wt_prov = '851' THEN '85101'
             WHEN s.wt_prov = '250' THEN '25002'  ELSE '' END
       ,''
       ,CASE WHEN s.wt_prov = '280' THEN 'AOR20221117008801'
             WHEN s.wt_prov = '100' THEN 'AOR20221129001701'
             WHEN s.wt_prov = '551' THEN 'AOR20221129002501'
             WHEN s.wt_prov = '771' THEN 'AOR20221130010001'
             WHEN s.wt_prov = '851' THEN 'AOR20221201000101'
             WHEN s.wt_prov = '250' THEN 'AOR20221212011101'  ELSE '' END
       ,concat('{"ZLG":{"xtcll":',s.pv,'}}')
       ,'{"ZLG":{"ZSYY":{"xtcll":0},"FZSYY":{"xtcll":0}}}'
FROM
(
     SELECT  wt_prov
            ,COUNT(1) AS pv
     FROM ham_jituan.dwd_client_event_di
     WHERE dt = '${DT}'
     GROUP BY  wt_prov
)s
INNER JOIN ham.dim_capacity_user_code m
ON s.wt_prov = m.code
;
insert into table ham_jituan.ads_zhzt_amt_di partition (dt='${DT}')
SELECT  regexp_replace('${DT}','-','')
       ,'B99999999999'
       ,'A202900002'
       ,'S10291200039'
       ,m.name
       ,m.code
       ,CASE WHEN s.wt_prov = '280' THEN '全息作战体系'
             WHEN s.wt_prov = '100' THEN '插码数据能力'
             WHEN s.wt_prov = '551' THEN '安徽移动插码系统'
             WHEN s.wt_prov = '771' THEN '中国移动广西APP'
             WHEN s.wt_prov = '851' THEN '通用能力平台'
             WHEN s.wt_prov = '250' THEN '和洞察对外大数据服务平台'  ELSE '' END
       ,CASE WHEN s.wt_prov = '280' THEN '28001007'
             WHEN s.wt_prov = '100' THEN '10001103'
             WHEN s.wt_prov = '551' THEN '55101079'
             WHEN s.wt_prov = '771' THEN '77101040'
             WHEN s.wt_prov = '851' THEN '85101022'
             WHEN s.wt_prov = '250' THEN '25002216'  ELSE '' END
       ,CASE WHEN s.wt_prov = '280' THEN '四川中台能力运营中心'
             WHEN s.wt_prov = '100' THEN '北京中台能力运营中心'
             WHEN s.wt_prov = '551' THEN '安徽中台能力运营中心'
             WHEN s.wt_prov = '771' THEN '广西中台能力运营中心'
             WHEN s.wt_prov = '851' THEN '贵州中台能力运营中心'
             WHEN s.wt_prov = '250' THEN '江苏中台能力运营中心'  ELSE '' END
       ,CASE WHEN s.wt_prov = '280' THEN '28001'
             WHEN s.wt_prov = '100' THEN '10001'
             WHEN s.wt_prov = '551' THEN '55101'
             WHEN s.wt_prov = '771' THEN '77101'
             WHEN s.wt_prov = '851' THEN '85101'
             WHEN s.wt_prov = '250' THEN '25002'  ELSE '' END
       ,''
       ,CASE WHEN s.wt_prov = '280' THEN 'AOR20221117008801'
             WHEN s.wt_prov = '100' THEN 'AOR20221129001701'
             WHEN s.wt_prov = '551' THEN 'AOR20221129002501'
             WHEN s.wt_prov = '771' THEN 'AOR20221130010001'
             WHEN s.wt_prov = '851' THEN 'AOR20221201000101'
             WHEN s.wt_prov = '250' THEN 'AOR20221212011101'  ELSE '' END
       ,concat('{"ZLG":{"xtcll":',s.pv,'}}')
       ,'{"ZLG":{"ZSYY":{"xtcll":0},"FZSYY":{"xtcll":0}}}'
FROM
(
     SELECT  wt_prov
            ,COUNT(1) AS pv
     FROM ham_jituan.dwd_client_event_di
     WHERE dt = '${DT}'
     and wt_prov in ('280','100','551','771','851','250')
     GROUP BY  wt_prov
)s
INNER JOIN ham.dim_capacity_user_code m
ON s.wt_prov = m.code
;
