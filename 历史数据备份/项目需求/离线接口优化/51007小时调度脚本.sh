#!/bin/bash 

Njob=24 #任务总数 
Nproc=12 #最大并发进程数 
daytime=$1 #数据日期
outputpath=$2 #输出目录
day=$3
file=$4
name=$5

ago=$(date -d"15 day ago" +%Y%m%d)
echo "清理文件"
cd $outputpath

rm -rf ${ago}
rm -rf ${day}
echo "创建文件夹"
mkdir -p ${day}
cd ${day}


time1=$(date "+%Y-%m-%d %H:%M:%S")
echo "开始第"$hour"小时文件写出_当前时间"$time1

sh $file $daytime $hour | sed 's/"//g' | split -a 3 --additional-suffix=.dat -d -l 800000 -d --numeric-suffixes=1 - 510_"$hour"_

time2=$(date "+%Y-%m-%d %H:%M:%S")
echo "开始第"$hour"小时文件转码_当前时间"$time2
origz=$(ls 510_"$hour"_*.dat 2> /dev/null)
filecount=1
for g in ${origz};do
  cat $g | iconv -c -t GBK | sed "s/\x01/\x80/g"  >  a_10000_"$hour"_"$name"_00_"$filecount".dat
  filecount=$[$filecount + 1]
done
rm -f 510_"$hour"_*.dat


time3=$(date "+%Y-%m-%d %H:%M:%S")
echo "开始第"$hour"小时文件压缩_当前时间"$time3
touch a_10000_"$day"_"$name"_00.verf
dats=$(ls a_10000_"$hour"_*.dat 2> /dev/null)
for f in ${dats}; do
    echo "$f.gz,$(wc -c < $f),$(wc -l < $f),$day,$(date +%Y%m%d%H%M%S)" | iconv -t GBK | sed "s/\\x2c/\\x80/g" >> a_10000_"$day"_"$name"_00.verf
    gzip -f $f
    sleep 3
  done


echo "开始生成校验文件"
filecount1=1
for i in $(ls *.dat.gz); do
  if [ $filecount1 -lt 10 ];then
     mv $i a_10000_"$day"_"$name"_00_000"$filecount1".dat.gz
     sed -i "s/$i/a_10000_"$day"_"$name"_00_000"$filecount1".dat.gz/g" a_10000_"$day"_"$name"_00.verf
  elif [ $filecount1 -lt 100 ];then
     mv $i a_10000_"$day"_"$name"_00_00"$filecount1".dat.gz
     sed -i "s/$i/a_10000_"$day"_"$name"_00_00"$filecount1".dat.gz/g" a_10000_"$day"_"$name"_00.verf
  elif [ $filecount1 -lt 1000 ];then
     mv $i a_10000_"$day"_"$name"_00_0"$filecount1".dat.gz
     sed -i "s/$i/a_10000_"$day"_"$name"_00_0"$filecount1".dat.gz/g" a_10000_"$day"_"$name"_00.verf
  else
     mv $i a_10000_"$day"_"$name"_00_"$filecount1".dat.gz
     sed -i "s/$i/a_10000_"$day"_"$name"_00_"$filecount1".dat.gz/g" a_10000_"$day"_"$name"_00.verf
  fi
  filecount1=$[$filecount1 + 1]
done

#for i in $(ls *.dat.gz); do
#  CNT=$(zcat $i | wc -l)
#  echo "$i,$(wc -c < $i),$CNT,$day,$(date -d @$(stat -c %Y $i) +%Y%m%d%H%M%S)" | iconv -t GBK | sed "s/\\x2c/\\x80/g" >> a_10000_"$day"_"$5"_00.verf
#done
 

echo -e "time-consuming: $SECONDS   seconds"    #显示脚本执行耗时