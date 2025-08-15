[udbac@logs-api01 ~]$ crontab -l
# 57 00 * * * /home/udbac/bin/crontab/51006_auto.sh
*/5 * * * *  /bin/bash /home/udbac/xq_test/ck_check.sh >> /dev/null 2>&1
*/50 * * * *  /bin/bash /home/udbac/xq_test/check_ck_airflow.sh >> /dev/null 2>&1
30 23 * * *  /bin/bash /home/udbac/xq_test/find_rm_afhachidaily.sh >> /dev/null 2>&1



##第一个--/home/udbac/bin/crontab/51006_auto.sh
#! /bin/bash

date=$(date +"%FT%T")
echo "*****开始*****  $(date +"%Y-%m-%d %H:%M:%S")" >> /home/udbac/crontab_log/51006_"${date}".txt
cd /home/udbac/51006/incoming
while true; do
	sleep 5
    if [ -e count.txt ]; then break; fi
        sleep 295
done

echo "执行分割脚本  $(date +"%Y-%m-%d %H:%M:%S")" >> /home/udbac/crontab_log/51006_"${date}".txt
sh /home/udbac/bin/51006_auto_zhuanhuan_push.sh

echo "完成  $(date +"%Y-%m-%d %H:%M:%S")" >> /home/udbac/crontab_log/51006_"${date}".txt

##第二个 --/home/udbac/xq_test/check_ck_airflow.sh  
#!/bin/bash
f=`date +%F`
fhour=`date +%H`
if [[ $fhour == 00 ]];then

exit
fi
dat1ago=`date -d "1hour ago" +%H`

 clickhouse-client -h 10.104.81.165 -m --receive_timeout=1200 -q "select toHour(event_time),count(1) from olap.event_all where toDate(event_time) = '$f' group by toHour(event_time) order by toHour(event_time);"  >ck_old

sleep 5


 clickhouse-client -h 10.104.81.165 -m --receive_timeout=1200 -q "select toHour(event_time),count(1) from olap.event_all where toDate(event_time) = '$f' group by toHour(event_time) order by toHour(event_time);"  >ck_new

ckdate=`diff ck_old ck_new  | awk '{print $2} ' | grep -vE ^$ | sort -u | head -1`
let ckdatelast=ckdate-1

let datelast=dat1ago-ckdatelast
if [ $datelast -gt 2  ];then
   echo "ck库数据延迟1小时以上";
#   bash /home/udbac/xq_test/dingding.sh "ck-warning-ck库数据延迟一小时以上" 
fi

##第三个-/home/udbac/xq_test/check_ck_airflow.sh
#!/bin/bash
f=`date +%F`
fhour=`date +%H`
if [[ $fhour == 00 ]];then

exit
fi
dat1ago=`date -d "1hour ago" +%H`

 clickhouse-client -h 10.104.81.165 -m --receive_timeout=1200 -q "select toHour(event_time),count(1) from olap.event_all where toDate(event_time) = '$f' group by toHour(event_time) order by toHour(event_time);"  >ck_old

sleep 5


 clickhouse-client -h 10.104.81.165 -m --receive_timeout=1200 -q "select toHour(event_time),count(1) from olap.event_all where toDate(event_time) = '$f' group by toHour(event_time) order by toHour(event_time);"  >ck_new

ckdate=`diff ck_old ck_new  | awk '{print $2} ' | grep -vE ^$ | sort -u | head -1`
let ckdatelast=ckdate-1

let datelast=dat1ago-ckdatelast
if [ $datelast -gt 2  ];then
   echo "ck库数据延迟1小时以上";
#   bash /home/udbac/xq_test/dingding.sh "ck-warning-ck库数据延迟一小时以上" 
fi
[udbac@logs-api01 ~]$ cat /home/udbac/xq_test/find_rm_afhachidaily.sh
#!/bin/bash
cd /home/udbac/output/af_hachi_daily

for j in  `find ./ -type d -mtime +5 | awk -F/ '{print $2}' | sort -u`
do
    rm -rf /home/udbac/output/af_hachi_daily/$j
#     ls -ld /home/udbac/output/af_hachi_daily/$j
done


#for i in `ls`
#for i in 51008_daily
#do
#  cd /home/udbac/output/$i
#  find . -type d -mtime +3 | xargs ls
#done


#第四个--/home/udbac/xq_test/find_rm_afhachidaily.sh
#!/bin/bash
cd /home/udbac/output/af_hachi_daily

for j in  `find ./ -type d -mtime +5 | awk -F/ '{print $2}' | sort -u`
do
    rm -rf /home/udbac/output/af_hachi_daily/$j
#     ls -ld /home/udbac/output/af_hachi_daily/$j
done


#for i in `ls`
#for i in 51008_daily
#do
#  cd /home/udbac/output/$i
#  find . -type d -mtime +3 | xargs ls
#done
[udbac@logs-api01 ~]$ cat /home/udbac/xq_test/find_rm_afhachidaily.sh
#!/bin/bash
cd /home/udbac/output/af_hachi_daily

for j in  `find ./ -type d -mtime +5 | awk -F/ '{print $2}' | sort -u`
do
    rm -rf /home/udbac/output/af_hachi_daily/$j
#     ls -ld /home/udbac/output/af_hachi_daily/$j
done


#for i in `ls`
#for i in 51008_daily
#do
#  cd /home/udbac/output/$i
#  find . -type d -mtime +3 | xargs ls
#done



#69-root#!/bin/sh
. /etc/profile
. ~/.bash_profile
time=$(date -d "1 minute ago" +"%Y-%m-%d %H:%M:00")
dat=$(date -d "1 minute ago" +"%Y%m%d%H%M00")
psql "host=10.253.182.68 port=6500 user=clsuser  password=cls@123 dbname=cls_prod" << EOF
\COPY (select to_char(now(),'yyyyMMddHH24missms'),to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss') - INTERVAL '10 min','yyyyMMddHH24miss'),to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss'),'yyyyMMddHH24miss'),'KHDJY','OTHER','999999','TYRZ',case when page='我的' then '01' when page='探索' then '02' when page='话费余额' then '03' when page='首页' then '04' when page='商城' then '05'          when page='套餐余量' then '06' when page='流量查询' then '07' when page='已订业务' then '08' when page='账单查询' then '09' else '' end as codes,'TYRZ','999999','000','PM-KHDJY-OTHER-01-003-000',uv,'','','','','' from cls_quality_monitor_ind where window_end='$time' union all select to_char(now(),'yyyyMMddHH24missms'),to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss') - INTERVAL '10 min','yyyyMMddHH24miss'),to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss'),'yyyyMMddHH24miss'),'KHDJY','OTHER','999999','TYRZ',case when page='我的' then '01' when page='探索' then '02' when page='话费余额' then '03' when page='首页' then '04' when page='商城' then '05' when page='套餐余量' then '06' when page='流量查询' then '07' when page='已订业务' then '08' when page='账单查询' then '09' else '' end as codes,'TYRZ','999999','000','PM-KHDJY-OTHER-07-003-000',round(cast(load_avg as numeric)/cast(1000 as numeric),2),'','','','','' from cls_quality_monitor_ind where window_end='$time' union all select to_char(now(),'yyyyMMddHH24missms'),to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss') - INTERVAL '10 min','yyyyMMddHH24miss'),to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss'),'yyyyMMddHH24miss'),'KHDJY','OTHER','999999','TYRZ',case when page='我的' then '01' when page='探索' then '02' when page='话费余额' then '03' when page='首页' then '04' when page='商城' then '05' when page='套餐余量' then '06' when page='流量查询' then '07' when page='已订业务' then '08' when page='账单查询' then '09' else '' end as codes,'TYRZ','999999','000','PM-KHDJY-OTHER-07-002-000',round(cast(access_avg as numeric)/cast(1000 as numeric),2) ,'','','','','' from cls_quality_monitor_ind where window_end='$time') TO '/home/udbac/zhil.csv' (format csv, delimiter '|')
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


01 * * * * /bin/sh /home/udbac/export.sh
11 * * * * /bin/sh /home/udbac/export.sh
21 * * * * /bin/sh /home/udbac/export.sh
31 * * * * /bin/sh /home/udbac/export.sh
41 * * * * /bin/sh /home/udbac/export.sh
51 * * * * /bin/sh /home/udbac/export.sh