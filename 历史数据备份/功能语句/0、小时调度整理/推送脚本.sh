#!/bin/bash

dt=$1
dir=$2
cd $dir

start_time=$(date +"%Y-%m-%d %H:%M:%S")
echo "推送文件开始"
echo $start_time

#当校验文件行数和gz文件个数不一致时退出
if [ ${file_count} -ne ${verfcount} ]; then
  exit -1
fi

Nproc=4 #数据上传最大并发进程数
# 等待进程时间
pause_time=1

# gz文件个数
file_count=$(ls *gz | wc -l)
echo "上传gz总文件数：$file_count"
# verf文件行数
verfcount=$(cat *.verf | wc -l) 
echo ${verfcount}
# 单批次文件数
#固定文件数
#batch_size=300
#动态计算文件数
batch_size=$((file_count/4))

# 计算gz文件任务总数
Njob=$((file_count/batch_size))
if (( file_count%batch_size > 0 )); then
  ((Njob++))
fi
echo "上传总任务数：$Njob"


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


function upload_files() {
    # 创建新的线程
    # local file_lines_end=$((current_proc_num*batch_size))
    # local files=$(ls *.gz | sort | head -n $file_lines_end | tail -n $batch_size | tr '\n' ' '  2> /dev/null)
    local file_lines_start=$((file_count-(current_proc_num-1)*batch_size))
    local files=$(ls *.gz | sort | tail -n $file_lines_start | head -n $batch_size | tr '\n' ' '  2> /dev/null)
    echo $files
    lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3968 sftp://10.252.180.2/incoming/cmccsales_270cm/  -e "set xfer:log-file $dir/upload.log; mput -c $files; exit;" 
   # lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3968 sftp://10.252.180.2/incoming/tmp1124/  -e "set xfer:log-file $dir/upload.log; mput -c $files; exit;" 
    

}

function upload_verf() {
    lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3968 sftp://10.252.180.2/incoming/cmccsales_270cm/  -e "mput -c *.verf; exit;" 
   # lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3968 sftp://10.252.180.2/incoming/tmp1124/  -e "mput -c *.verf; exit;" 
    echo "推送verf文件结束"
}

# function find_missing_files() {
  
# }

function start_upload() {
  
  # 清空上传列表日志
  > $dir/upload.log
  # 当前进程数
  current_proc_num=1
  while (( current_proc_num <= Njob ));do 

      upload_files &
      PID=$!

      PushQue $PID

      while [[ $Nrun -ge $Nproc ]]; do          # 如果Nrun大于Nproc，就一直ChkQue
           #echo "chkQue,Nrun: $Nrun"
           ChkQue

           sleep 1

      done
      ((current_proc_num++))
  done

  wait
  echo "推送gz文件结束"
  echo "已完成文件："
  cat $dir/upload.log | awk -F ' ' '{print $3}' | awk -F '/' '{print $6}' | sort
  success_count=$(cat $dir/upload.log | wc -l)
  echo "成功推送文件数$success_count"
  upload_verf
}


function insert_data() {
    
end_time=$(date +"%Y-%m-%d %H:%M:%S")
echo "推送文件结束"
echo $end_time


SQL_end="

INSERT OVERWRITE TABLE ham_jituan.data_job_monitoring partition (dt = '${dt}',hour = '23',job_name = 'push_51007_daily')
values ('51007','${start_time}','${end_time}','','','','','运行完毕');

"

beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac  -e "$SQL_end"


}
# 开始推送
start_upload
# 推送时长写入数据库
insert_data