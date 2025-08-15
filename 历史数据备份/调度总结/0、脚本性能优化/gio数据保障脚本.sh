#! /bin/bash

select_num=0
#睡30分钟
#查询ck数据量
echo "查询ck小时数据量"
#sql查询数据量
befor_num=$(clickhouse-client -h 10.253.248.77 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="select count(1) from olap.event_all where toDate(event_time)='2023-08-28' and toHour(event_time)=2 FORMAT CSV") 
#ck获取数据表数据量
echo $befor_num

sleep 1
while true
do
    select_num=$((select_num+1))
    sleep $((select_num*5))
    echo "第$select_num 次复查ck小时数据量"
    #after_num=$(($(clickhouse-client -h 10.253.248.77 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="select count(1) from olap.event_all where toDate(event_time)='2023-08-28' and toHour(event_time)=2 FORMAT CSV")+$select_num))
    after_num=$(clickhouse-client -h 10.253.248.77 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="select count(1) from olap.event_all where toDate(event_time)='2023-08-28' and toHour(event_time)=2 FORMAT CSV")
    echo $after_num
    if [ $after_num -le $befor_num ] && [ $after_num -ne 0 ]
    then
        echo '执行'
        break;
    fi
    befor_num=$after_num
done


