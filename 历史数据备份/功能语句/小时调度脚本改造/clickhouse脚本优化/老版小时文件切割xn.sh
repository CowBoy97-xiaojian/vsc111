#!/bin/bash

source /etc/profile

CNT=800000

dfmt=$1
DT=$3
Hour=$4

start_time=$(date +"%Y-%m-%d %H:%M:%S")
echo "生成文件开始"
echo $start_time

ago=$(date -d"15 day ago" +%Y%m%d)
echo "清理文件"
rm -rf ${ago}
rm -rf $1/$1_$Hour
echo "创建小时文件夹"
mkdir -p $1/$1_$Hour
cd $1/$1_$Hour

SQL_start="
INSERT INTO TABLE ham_jituan.data_job_monitoring partition (dt = '${DT}',hour = '${Hour}',job_name = 'export_51007_gio_hourly')
values ('51007','${start_time}','','','','','unknown','正在运行');
"
beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac  -e "$SQL_start"


# 目前脚本各个进程需要时间：转换txt 1小时20分钟
# 转换分割 2个半小时
# 压缩 1小时50分钟
# 生成校验文件 40分钟

nproc=20  # 并行压缩进程数
tries=5   # 整体尝试次数

echo "0：拉取数据文件"
while true;do
  hdfs dfs -get /user/hive/warehouse/ham_jituan.db/ads_hachi_jzyy_xtb2_51007_gio_dt_hour/dt=$DT/hour=$Hour/*

# 如压缩文件和解压缩文件数据一致，则退出
  fc_gz=$(ls 00*.gz | wc -l)
  if [ ${fc_gz} -ne 0 ]; then 
    echo "拉取数据文件成功"
    break; 
  fi


  # 如还有压缩文件，继续解压转换
  tries=$[$tries - 1]
  if [ ${tries} -eq 0 ]; then 
    echo "继续步骤0"
    break; 
  fi
  sleep 1
done
echo "获取原始数据文件个数"
fc_orig=$(ls 00*.gz | wc -l)
echo ${fc_orig}

echo "1：解压缩、转换字符集"

while true; do
  # 获取所有原始压缩数据文件
  for f in $(ls 00*.gz 2> /dev/null); do
    # 解压缩、转换字符集
    while true; do
      np=$(ps aux | grep -P -- "iconv -ct GBK" | grep -v grep | wc -l)
      if [ $np -lt $nproc ]; then
        break;
      fi
      sleep 1
    done

    # 解压缩、转换字符集，如成功则删除原始压缩文件
    (zcat $f | iconv -ct GBK > new_$f.txt)&
  done

  # 等待尚未压完的进程
  while true; do
    np=$(ps aux | grep -P -- "iconv -ct GBK" | grep -v grep | wc -l)
    if [ $np -eq 0 ]; then
      break;
    fi
    sleep 1
  done

   # 如已没有压缩文件，则退出
  #origz=$(ls 00*.gz 2> /dev/null)
  #if [ "${origz}" = "" ]; then break; fi
  # 如压缩文件和解压缩文件数据一致，则退出
  fc_unzip=$(ls new_*.txt | wc -l)
  if [ ${fc_unzip} -eq ${fc_orig} ]; then 
    echo "压缩文件和解压缩文件数据一致"
    break; 
  fi


  # 如还有压缩文件，继续解压转换
  tries=$[$tries - 1]
  if [ ${tries} -eq 0 ]; then 
    echo "继续步骤1"
    break; 
  fi
done

echo "1.1：删除原始压缩文件"

for f in $(ls 00*.gz 2> /dev/null); do
  echo "删除$f"
  rm -f $f
done


echo "2：文件标记行号、分割"
while true;do
cat new_*.txt | awk '{print "00" "#_#" $0}'| sed "s/\\x23\\x5F\\x23/\\x80/g" | split -a 4 --additional-suffix=.dat -d -l $CNT --numeric-suffixes=1 - a_10000_"$1"_"$2"_"$Hour"_00_
split_file_count=$(ls a_10000_* | wc -l)
  if [ ${split_file_count} -ne 0 ];then
    echo "已形成分割文件"
    rm -f new_*.txt
    break;
  fi

  # 如果没有形成分割文件，则再次执行分割
  tries=$[$tries - 1]
  if [ ${tries} -eq 0 ]; then 
    echo "继续步骤2"
    break; 
  fi
done

echo "3：压缩"

# 对所有数据文件进行压缩，如失败，则延迟后再尝试
while true; do
  # 获取所有未压缩的文件，如无未压缩文件则退出
  for f in $(ls *.dat 2> /dev/null); do
    # 检查当前压缩的进程数
    while true; do
      np=$(ps aux | grep -P -- "gzip -1f.*.dat" | grep -v grep | wc -l)
      if [ $np -lt $nproc ]; then
        break;
      fi
      sleep 1
    done

    # 已有空闲进程，启动压缩
    (echo $f; gzip -1f $f;)&
  done

  # 等待尚未压完的进程
  while true; do
    np=$(ps aux | grep -P -- "gzip -1f.*.dat" | grep -v grep | wc -l)
    if [ $np -eq 0 ]; then
      break;
    fi
    sleep 1
  done

  # 如已无数据文件要压缩，则退出
  dats=$(ls *.dat 2> /dev/null)
  if [ "${dats}" = "" ]; then break; fi

  # 如还有数据文件，继续进行压缩
  tries=$[$tries - 1]
  if [ ${tries} -eq 0 ]; then 
    echo "继续步骤3"
    break; 
  fi
done

# echo "获取转换后的数据文件行数"
# lc_conv=$(zcat *.dat.gz | wc -l)
# echo ${lc_conv}
# 如行数不一致，则退出
# if [ ${lc_orig} -ne ${lc_conv} ]; then
#  exit -1
# fi



echo "4：开始生成校验文件"
for i in $(ls *.dat.gz); do
  if [[ $i==$(ls *.dat.gz | tail -1) ]]; then
     CNT=$(zcat $i | wc -l)
  fi
  echo "$i,$(wc -c < $i),$CNT,$1,$(date -d @$(stat -c %Y $i) +%Y%m%d%H%M%S)" | iconv -t GBK | sed "s/\\x2c/\\x80/g" >> a_10000_"$1"_"$2"_"$Hour"_00.verf
done

#将校验文件数据量存入文件

export_sum=$(cat *verf | sed 's/\x80/,/g' | gawk -F"," '{ sum += $3 }; END { print sum }')
echo $export_sum > export_"$1"_"$2"_"$Hour".log

echo "5:将校验文件数据量存入文件-$export_sum"

SQL="
select
count(1)
from ham_jituan.ads_hachi_jzyy_xtb2_51007_gio_dt_hour
where dt = '${DT}'
and hour = '${Hour}';


"
#获取hive中间表数据量
hive_sum=$(beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac  -e "$SQL")



#将hive文件数据量存入文件

echo "6:hive文件数据量存入文件-$hive_sum"

echo $hive_sum > hive_"$1"_"$2"_"$Hour".log 

data_consistency='false'

echo "7:生成标志"
export_dev=$((export_sum-hive_sum))
if [ ${export_dev} -gt -50 -a ${export_dev} -lt 50 -a ${export_sum} -ne 0 ];then
   echo "export_dev is $export_dev"
   echo "true" > flag_"$1"_"$Hour".log
   data_consistency='true'
fi

echo ${data_consistency}

#if [ "$export_sum" -eq "$hive_sum" ]
#then
#  echo "true" > flag_gio_"$1"_"$Hour".log
#fi

end_time=$(date +"%Y-%m-%d %H:%M:%S")
echo "生成文件结束"
echo $end_time

file_count=$(find /data/output/51007_gio_hourly/${dfmt}/${dfmt}_${Hour} -name "*.gz" | wc -l )

SQL_end="

INSERT OVERWRITE TABLE ham_jituan.data_job_monitoring partition (dt = '${DT}',hour = '${Hour}',job_name = 'export_51007_gio_hourly')
values ('51007','${start_time}','${end_time}','${file_count}','${export_sum}','${hive_sum}','${data_consistency}','运行完毕');

"

beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac  -e "$SQL_end"