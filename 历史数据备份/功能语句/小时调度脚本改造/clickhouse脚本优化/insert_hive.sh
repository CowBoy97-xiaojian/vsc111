#!/bin/bash 
Njob=5 #任务总数  
Nproc=5 #数据写出最大并发进程数
file=/home/udbac/bin/ckout_51007_sql.sh #sql脚本文件
#ck_dt=$2 #ck event_all分区日期，eq:20230704
hive_dt=$1 #hive分区日期，eq:2023-07-04
hour=$2 #hive分区小时，eq:01
ck_dt=$(echo $hive_dt | sed "s/-//g") #ck event_all分区日期，eq:20230704

#hosts_group1=("10.253.248.73" "10.104.81.183" "10.104.81.171" "10.104.82.66" "10.104.82.73") #ck集群分片节点
#hosts_group2=("10.253.248.77" "10.104.81.167" "10.104.81.165" "10.104.82.69" "10.104.82.67") #ck集群副本节点


function PushQue {      #将PID值追加到队列中

           Que="$Que $1"

           Nrun=$(($Nrun+1))

}

function GenQue {       #更新队列信息，先清空队列信息，然后检索生成新的队列信息

           OldQue=$Que

           Que=""; Nrun=0

           for PID in $OldQue; do

                 if [[ -d /proc/$PID ]]; then

                        PushQue $PID

                 fi

           done

}


function ChkQue {       #检查队列信息，如果有已经结束了的进程的PID，那么更新队列信息

           OldQue=$Que

           for PID in $OldQue; do

                 if [[ ! -d /proc/$PID ]];   then

                 GenQue; break

                 fi

           done

}



function data_insert(){

time1=$(date "+%Y-%m-%d %H:%M:%S")
echo "开始第"$ck_dt"_"$hour"_"$ip"小时数据写出_当前时间"$time1
sh $file $ip $ck_dt $hour
time2=$(date "+%Y-%m-%d %H:%M:%S")
echo "开始第"$ck_dt"_"$hour"_"$ip"小时数据上传HDFS_当前时间"$time2
hdfs dfs -put /data/datahql/data_ck_"$ck_dt"_"$hour"_"$ip".csv /user/hive/ck_out/jituan
rm -f /data/datahql/data_ck_"$ck_dt"_"$hour"_"$ip".csv
time3=$(date "+%Y-%m-%d %H:%M:%S")
echo "完成第"$ck_dt"_"$hour"_"$ip"小时数据上传HDFS_当前时间"$time3
}



# modified_str=${hour#*0}
# hour_trans=`echo $modified_str | awk -F '' '{print $1}'`
hour_trans=`echo $hour | awk -F '' '{print $2}'`
echo "小时："$hour_trans
if (( hour_trans % 2 == 0 )); then
  echo '节点:73、183、171、66、73'
  hosts=("10.253.248.73" "10.104.81.183" "10.104.81.171" "10.104.82.66" "10.104.82.73")  #ck集群分片节点
else
  echo '节点:77、167、165、69、67'
  hosts=("10.253.248.77" "10.104.81.167" "10.104.81.165" "10.104.82.69" "10.104.82.67")  #ck集群副本节点
fi
echo "删除ck上传文件"
hdfs dfs -rm -r -f /user/hive/ck_out/jituan/data_ck_${ck_dt}_${hour}_*

for t in ${hosts[*]};do

      ip=$t

      data_insert &

      PID=$!

      PushQue $PID

      while [[ $Nrun -ge $Nproc ]]; do          # 如果Nrun大于Nproc，就一直ChkQue

           ChkQue

           sleep 0.1

      done

done

wait

sql_del="alter table ham_jituan.ods_client_event_gio_all drop partition(dt='$hive_dt',hour='$hour')"
echo "先删除$hive_dt，$hour分区数据"
beeline -n udbac -u jdbc:hive2://master01:10000/ham -e "$sql_del"
echo "删除分区文件"
hdfs dfs -rm -r -f /user/hive/warehouse/ham_jituan.db/ods_client_event_gio_test/dt=$hive_dt/hour=$hour/*
sql="load data inpath 'hdfs://udbachdp1/user/hive/ck_out/jituan/data_ck_${ck_dt}_${hour}_*' into table ham_jituan.ods_client_event_gio_all partition(dt='$hive_dt',hour='$hour')"
echo "开始hivesql导入数据"
beeline -n udbac -u jdbc:hive2://master01:10000/ham -e "$sql"


time4=$(date "+%Y-%m-%d %H:%M:%S")
echo "完成第"$ck_dt"_"$hour"小时数据导入HIVE_当前时间"$time4