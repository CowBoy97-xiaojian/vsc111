#!/bin/bash

dt=$1

start_time=$(date +"%Y-%m-%d %H:%M:%S")
echo "推送文件开始"
echo $start_time

# 转换后*.dat.gz文件数量
gzcount=$(ls *.dat.gz | wc -l)

echo ${gzcount}

# verf文件行数
verfcount=$(cat *.verf | wc -l) 

echo ${verfcount}

#当校验文件行数和gz文件个数不一致时退出
if [ ${gzcount} -ne ${verfcount} ]; then
  exit -1
fi

echo "push start"

upload_file() {
  local file=$1
  #最大尝试次数
  local max_retries=3
  #失败是延迟5秒
  local retry_delay=5
  #尝试次数
  local retry_count=0
  
  while [ $retry_count -lt $max_retries ]; do
    #lftp -u 'user,password' -p 9999 sftp://192.168.21.2:/opt/install/ -e "put $file -o /opt/install/;exit;"
    #CM系列接口上传路径
    #lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3964 sftp://10.252.180.2/incoming/cmpt/ -e "mput -c $file; exit;"
    #51系列上传路径
    lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3964 sftp://10.252.180.2/incoming/cmccsales_270cm/  -e "mput -c $file; exit;"

    if [ $? -eq 0 ]; then
      echo "文件 '$file' 上传成功"
      return 0
    else
      echo "文件 '$file' 上传失败，正在进行重试..."
      retry_count=$((retry_count+1))
      sleep $retry_delay
    fi
  done
  
  echo "文件 '$file' 上传失败，已达到最大重试次数:$retry_count"
  return 1
}

#成功gz文件个数
success_count=0

#成功verf文件个数
success_count_verf=0

echo "开始上传gz文件"

for file in $(ls *gz 2> /dev/null); do
  if upload_file "$file"; then
    success_count=$((success_count+1))
  fi
done

echo "总共上传成功 $success_count 个gz文件"

echo "开始上传 verf校验文件"

for file in $(ls *verf 2> /dev/null); do
  if upload_file "$file"; then
    success_count_verf=$((success_count_verf+1))
  fi
done

echo "总共上传成功 $success_count_verf 个verf文件"

echo "push succeed"

end_time=$(date +"%Y-%m-%d %H:%M:%S")
echo "推送文件结束"
echo $end_time

SQL_end="

INSERT OVERWRITE TABLE ham_jituan.data_job_monitoring partition (dt = '${dt}',hour = '23',job_name = 'push_51006_daily')
values ('51006','${start_time}','${end_time}','','','','','运行完毕');

"

beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac  -e "$SQL_end"



