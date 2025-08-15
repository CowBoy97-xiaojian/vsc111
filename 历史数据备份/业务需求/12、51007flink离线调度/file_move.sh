#!/bin/bash
Njob=8 #任务总数
Nproc=8 #最大并发进程数
file_type=$1
source_hosts=("10.253.182.86" "10.253.182.78" "10.253.182.75" "10.253.182.80" "10.253.182.73" "10.253.182.82" "10.253.182.72" "10.253.182.74")
source_path=/data1/udbac/output_ck/"$file_type"_daily
target_path=/home/udbac/output_ck/"$file_type"_daily
#date_time_path=$(date -d "1 hour ago" +"%Y-%m-%d/%H")
#date_path=$(date -d "1 hour ago" +"%Y-%m-%d")
#date_hour=$(date -d "1 hour ago" +"%H")
date_time_path=$2
date_path=$3
date_hour=$4

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

function file_transform(){
	file_ct=$(ssh $ip "ls $source_path/$date_time_path/ 2> /dev/null | wc -l")
	if [ $file_ct -gt 0 ]; then
                time3=$(date "+%Y-%m-%d %H:%M:%S")
                echo "开始"$ip"主机文件迁移_当前时间"$time3
		source_file="$source_path"/"$date_path"/"$file_type"_"$ip"_"$date_hour".dat
    	        target_file="$file_type"_"$ip"_"$date_hour".dat.gz
    	        ssh $ip "cat $source_path/$date_time_path/* > $source_file"
    	        ssh $ip "gzip -f $source_file"
    	        md5_src=$(ssh $ip "md5sum $source_file.gz" | awk '{print $1}')
    	        if [ ! -d "${target_path}/${date_path}" ]; then
        	     mkdir -p "${target_path}/${date_path}"
    	        fi
    	        scp $ip:$source_file.gz ${target_path}/${date_path}
    	        md5_trg=$(md5sum ${target_path}/${date_path}/$target_file | awk '{print $1}')
		while [ "$md5_src" != "$md5_trg" ]; do
        	     scp $ip:$source_file.gz ${target_path}/${date_path}
        	     md5_trg=$(md5sum ${target_path}/${date_path}/$target_file | awk '{print $1}')
    	        done
                time4=$(date "+%Y-%m-%d %H:%M:%S")
                echo "完成"$ip"主机文件迁移_当前时间"$time4
        fi
}



for t in ${source_hosts[*]};do

	   ip=$t

           file_transform &

           PID=$!

           PushQue $PID

           while [[ $Nrun -ge $Nproc ]]; do          # 如果Nrun大于Nproc，就一直ChkQue

                 ChkQue

                 sleep 0.1

           done

done

wait

echo "删除生成的压缩文件"
source_file2="$source_path"/"$date_time_path
for t in ${source_hosts[*]};do

	   ip=$t

         scp $ip:$source_file.gz ${target_path}/${date_path}
      
         ssh $ip "rm -rf "$source_file2"/*.dat.gz"

done


time2=$(date "+%Y-%m-%d %H:%M:%S")
echo "完成"$date_time_path"小时所有文件迁移_当前时间"$time2
