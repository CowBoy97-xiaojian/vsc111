#! /bin/bash

DT="$1"
Hour="$2"
echo "当前查询时间:${DT} ${Hour}"

hdfs_dir="/user/hive/warehouse/ham_jituan.db/seatunnel_cdpevent/"
dt_hdfs_dir="${hdfs_dir}dt=${DT}/"
hour_hdfs_dir="${hdfs_dir}dt=${DT}/hour=${Hour}"

echo "Seatunnel数据监控开启"
before_hour=$(hdfs dfs -ls "${hour_hdfs_dir}/*" | tail -1 | awk -F" +" '{print $6,$7}')
before_timstamp_hour=$(date -d "${before_hour}" +%s)

before_day=$(hdfs dfs -ls "${hdfs_dir}/*/*" | tail -1 | awk -F" +" '{print $6,$7}')
before_timstamp_day=$(date -d "${before_day}" +%s)

echo "小时最新数据文件生成时间：${before_hour}"
echo "全天最新数据文件生成时间：${before_day}"
while true
do
    sleep 300
    echo "查询小时数据量"
    after_hour=$(hdfs dfs -ls "${hour_hdfs_dir}/*" | tail -1 | awk -F" +" '{print $6,$7}')
    after_timstamp_hour=$(date -d "${after_hour}" +%s)

    after_day=$(hdfs dfs -ls "${hdfs_dir}/*/*" | tail -1 | awk -F" +" '{print $6,$7}')
    after_timstamp_day=$(date -d "${after_day}" +%s)
    echo "小时最新数据文件生成时间：${after_hour}"
    echo "全天最新数据文件生成时间：${after_day}"
    if [ $before_timstamp_day -eq $after_timstamp_day ]
    then
        echo "紧急告警:seatunnel已停止入库"
        sh /data/bin/not_exits.sh
    elif [ $before_timstamp_hour -eq $after_timstamp_hour ] && [ $before_timstamp_day -lt $after_timstamp_day ]
    then
        echo "${DT} - ${Hour} 完成入库。"
        break;
    fi 
    echo "${DT} - ${Hour} 正在入库。"
    before_timstamp_hour=$after_timstamp_hour
    before_timstamp_day=$after_timstamp_day
done
