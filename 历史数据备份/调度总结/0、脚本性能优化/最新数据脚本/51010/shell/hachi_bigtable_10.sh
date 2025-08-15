#!/bin/bash

source /etc/profile

CNT=800000

DT=$3
Hour=$4

ago=$(date -d"5 day ago" +%Y%m%d)
echo "清理文件"
rm -rf ${ago}
rm -rf $1/$1_$Hour
echo "创建小时文件夹"
mkdir -p $1/$1_$Hour
cd $1/$1_$Hour
echo "拉取数据文件"
hdfs dfs -get /user/hive/warehouse/ham.db/ads_rpt_hachi_51010_hourly/dt=$DT/hour=$Hour/*

# echo "获取原始数据文件行数"
# lc_orig=$(zcat 00*.gz | wc -l)
# echo ${lc_orig}
echo "获取原始数据文件个数"
fc_orig=$(ls 00*.gz | wc -l)
echo ${fc_orig}
# 目前脚本各个进程需要时间：转换txt 1小时20分钟
# 转换分割 2个半小时
# 压缩 1小时50分钟
# 生成校验文件 40分钟

nproc=20  # 并行压缩进程数
tries=5   # 整体尝试次数



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
#  origz=$(ls 00*.gz 2> /dev/null)
#  if [ "${origz}" = "" ]; then break; fi
  fc_unzip=$(ls new_*.txt | wc -l)
  if [ ${fc_unzip} -eq ${fc_orig} ]; then 
    echo "压缩文件和解压缩文件数据一致"
    break; 
  fi


  # 如还有压缩文件，继续解压转换
  tries=$[$tries - 1]
  if [ ${tries} -eq 0 ]; then break; fi
done

echo "1.1：删除原始压缩文件"

for f in $(ls 00*.gz 2> /dev/null); do
  echo "删除$f"
  rm -f $f
done


echo "2：文件标记行号、分割"

cat new_*.txt | awk '{print "00" "#_#" $0}'| sed "s/\\x23\\x5F\\x23/\\x80/g" | split -a 3 --additional-suffix=.dat -d -l $CNT --numeric-suffixes=1 - a_10000_"$1"_"$2"_"$Hour"_00_
rm -f new_*.txt



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
  if [ ${tries} -eq 0 ]; then break; fi
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
sum(pv)
from (
select
count(1) as pv
from ham.ads_rpt_hachi_51006_dt_hour
where dt = '$DT'
and hour = '$Hour'
and trmnl_style in ('7c2d','3s3i','7p1j','3a5k','8z2s','1w7q','9w6z','8f4q','2i1b','6g7b','0d4d','8d2o','8m4a','5d3k','3z9h','4w6p','5f8e','3j1d','9o0p','0o1j','3z7i','9b5g','3r8r','3d2m','9m2x','8o8q','2v6r','8j5v','4z6i','9y7c','8s0b','1g3e','3n9u','6c9w','2n3t')
union all
select
count(1) as pv
from ham.dwd_dcslog_event_gio_di
where dt = '$DT'
and hour = '$Hour'
and trmnl_style in ('a797613d647af6c6','a7464be8b200fe24','b9e74b9d5da046ce','b87eb8cec01051a6','b098587a3af15a25','b23efeab1461679b','a10b1f67c7d288c1','ae2308d5a08258b6','a04f0ad9925c080f','a6ba03d6884ee1c1','aa3a7b170aa41eed','a441e7427a44d390','adae9b8274a853f1','ad2e89b1010aed1e','bfe72e945deea64f','81ebc780373ee7cd','8d5d4407d1c3f6bf','abd580f08cf381e0','ba81a7647a8bed5d','8c19b47f237e28d3','a609de73d1690c0e','9c294cf845907ac1','b0839305cf5d280d','9a238b92b6612604','afdbd0f6b9a7ba30','8273883304ad70fb','86fdf7160c5cc8cc','81fab8d01c17ee89','89e7161d4a6b817f','9b50668a016469ec','bda85eeb52fb8b87','a8c29f7abfd639b4','b8d9fd437de471e4','875444ff6e049f33','853d176d98834573','a1f48d9ff4f42571','b508a809cbbddd0b')
)t1;
"
#获取hive中间表数据量
hive_sum=$(beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac  -e "$SQL")



#将hive文件数据量存入文件

echo "6:hive文件数据量存入文件-$hive_sum"

echo $hive_sum > hive_"$1"_"$2"_"$Hour".log 


echo "7:生成标志"

if [ $export_sum -eq $hive_sum ]
then
  echo "true" > flag_"$1"_"$Hour".log 
fi
