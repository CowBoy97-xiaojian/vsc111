https://vnopj6kbg9.feishu.cn/docx/Q95Zdun6IovnRzxAOTQcvQxynlc


IT宁波
杭州集运



cat /home/udbac/export.sh

#!/bin/sh
. /etc/profile
. ~/.bash_profile
time=$(date -d "1 minute ago" +"%Y-%m-%d %H:%M:00")
dat=$(date -d "1 minute ago" +"%Y%m%d%H%M00")
psql "host=10.253.182.68 port=6500 user=clsuser  password=cls@123 dbname=cls_prod" << EOF
\COPY (
    select 
    to_char(now(),'yyyyMMddHH24missms'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss') - INTERVAL '10 min','yyyyMMddHH24miss'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss'),'yyyyMMddHH24miss'),
    'KHDJY',
    'OTHER',
    '999999',
    'TYRZ',
    case when page='我的' then '01' 
    when page='探索' then '02' 
    when page='话费余额' then '03' 
    when page='首页' then '04' 
    when page='商城' then '05'          
    when page='套餐余量' then '06' 
    when page='流量查询' then '07' 
    when page='已订业务' then '08' 
    when page='账单查询' then '09' 
    else '' end as codes,
    'TYRZ','999999',
    '000',
    'PM-KHDJY-OTHER-01-003-000',
    uv,'','','','','' 
    from cls_quality_monitor_ind 
    where window_end='$time' 
    union all 
    select 
    to_char(now(),'yyyyMMddHH24missms'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss') - INTERVAL '10 min','yyyyMMddHH24miss'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss'),'yyyyMMddHH24miss'),
    'KHDJY',
    'OTHER',
    '999999',
    'TYRZ',
    case when page='我的' then '01' 
    when page='探索' then '02' 
    when page='话费余额' then '03' 
    when page='首页' then '04' 
    when page='商城' then '05' 
    when page='套餐余量' then '06' 
    when page='流量查询' then '07' 
    when page='已订业务' then '08' 
    when page='账单查询' then '09' 
    else '' end as codes,
    'TYRZ','999999',
    '000',
    'PM-KHDJY-OTHER-07-003-000',
    round(cast(load_avg as numeric)/cast(1000 as numeric),2),
    '','','','','' 
    from cls_quality_monitor_ind
    where window_end='$time' 
    union all 
    select to_char(now(),'yyyyMMddHH24missms'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss') - INTERVAL '10 min','yyyyMMddHH24miss'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss'),'yyyyMMddHH24miss'),
    'KHDJY',
    'OTHER',
    '999999',
    'TYRZ',
    case when page='我的' then '01' 
    when page='探索' then '02' 
    when page='话费余额' then '03' 
    when page='首页' then '04'
    when page='商城' then '05' 
    when page='套餐余量' then '06' 
    when page='流量查询' then '07' 
    when page='已订业务' then '08' 
    when page='账单查询' then '09' 
    else '' end as codes,'TYRZ',
    '999999',
    '000',
    'PM-KHDJY-OTHER-07-002-000',
    round(cast(access_avg as numeric)/cast(1000 as numeric),2)
     ,'','','','','' 
     from cls_quality_monitor_ind where window_end='$time'
    ) TO '/home/udbac/zhil.csv' (format csv, delimiter '|')
\q
EOF
rm -f /home/udbac/zhong.csv
cat /home/udbac/zhil.csv|sed -r 's/""//g' >> /home/udbac/zhong.csv
cat /home/udbac/zhong.csv|sed -r 's/\|/#\|#/g' >> /home/udbac/KHDJY_01_10MI_$dat.csv
ftp -v -n 10.250.1.87<<EOF
user qwjk_ftp  E87qTL32dsXX!
binary
cd /data
lcd /home/udbac/
prompt
mput KHDJY_01_10MI_$dat.csv
bye
EOF
echo KHDJY_01_10MI_$dat.csv

#灰度环境
lftp -u 'pagerun,2wsx3EDC' -p 22 sftp://10.250.11.243/ngpsie/tyss/pagerun/workorder/ -e "mput -c /home/udbac/KHDJY_01_10MI_$dat.csv; exit;"

#生产环境
lftp -u 'pagerun,1qaz2wsx' -p 22 sftp://10.250.11.138/ngpsie/tyss/pagerun/workorder -e "mput -c /home/udbac/KHDJY_01_10MI_$dat.csv; exit;"

#灰度环境
lftp -u '' -p 22 sftp://10.250.11.243/ngpsie/tyss/pagerun/workorder/ -e "mput -c /home/udbac/KHDJY_01_10MI_$dat.csv; exit;"

#生产环境
lftp -u '' -p 22 sftp://10.250.11.138/ngpsie/tyss/pagerun/workorder -e "mput -c /home/udbac/KHDJY_01_10MI_$dat.csv; exit;"







