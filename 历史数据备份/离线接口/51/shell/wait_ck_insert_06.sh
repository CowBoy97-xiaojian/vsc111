#! /bin/bash

DT=$1
Hour=$2
select_num=0
#睡30分钟
#查询ck数据量
echo "查询ck小时数据量"
#sql查询数据量
befor_num=$(clickhouse-client -h 10.253.248.77 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="select count(1) from olap.event_all where toDate(event_time)='$DT' and LPAD(toString(toHour(event_time)), 2, '0')='$Hour' FORMAT CSV") 
#查询ch最新入库时间
befor_time=$(clickhouse-client -h 10.253.248.77 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="select max(event_time) from olap.event_all FORMAT CSV")
#ck获取数据表数据量
echo "befor_num:$befor_num,befor_time:$befor_time"
sleep 60
while true
do
    select_num=$((select_num+1))
    echo "第$select_num 次复查ck小时数据量"
    #after_num=$(($(clickhouse-client -h 10.253.248.77 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="select count(1) from olap.event_all where toDate(event_time)='2023-08-28' and toHour(event_time)=2 FORMAT CSV")+$select_num))
    after_num=$(clickhouse-client -h 10.253.248.77 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="select count(1) from olap.event_all where toDate(event_time)='$DT' and LPAD(toString(toHour(event_time)), 2, '0')='$Hour' FORMAT CSV")
#查询ch最新入库时间
    after_time=$(clickhouse-client -h 10.253.248.77 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="select max(event_time) from olap.event_all FORMAT CSV")     
    echo "after_num:$after_num,after_time:$after_time"
    if [ "$after_time" = "$befor_time" ]
    then
      	    echo "紧急告警:ch已停止入库"
	    sh /home/udbac/bin/not_exits.sh
    fi 
    if [ $after_num -le $befor_num ] && [ $after_num -ne 0 ]
    then
        echo '执行-ck2hive'
        sh /home/udbac/bin/insert_hive_H5.sh $DT $Hour
        break;
     fi
     befor_num=$after_num
     if [ $select_num -le 2 ]
     then 
	     sleep $((select_num * 300))
     else
	     sleep 600
     fi
done
