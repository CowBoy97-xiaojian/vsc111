#!/bin/bash

if [ "$#" -ne 4 ]; then
  echo "参数列表错误：（1）日期（yyyymmdd）（2）应当是CM001 （3）省份编码（4）省份拼音" >&2
  exit 1
fi

CNT=800000

ago=$(date -d"15 day ago" +%Y%m%d)
cd /home/udbac/output/af_hachi_daily
echo "清理文件"
rm -rf ${ago}/new_$2/$4
rm -rf $1/new_$2/$4
echo "创建文件夹"
mkdir -p $1/new_$2/$4
cd $1/new_$2/$4
echo "拉取数据文件"
hdfs dfs -get /user/hive/warehouse/ham_jituan.db/ads_hachi_"${2,,}"_shanghai/prov=$4/*

# echo "获取原始数据文件行数"
# lc_orig=$(zcat 00*.gz | wc -l)
# echo ${lc_orig}

echo "转换txt开始"
tries2=5   # 整体尝试次数
while true; do
  origz=$(ls 00*.gz 2> /dev/null)
  if [ "${origz}" = "" ]; then break; fi

for g in ${origz};do
        zcat $g | iconv -c -t GBK > new_$g.txt
        if [ -e new_$g.txt ]; then rm -f $g; fi
done

  tries2=$[$tries2 - 1]
  if [ ${tries2} -eq 0 ]; then break; fi
done

echo "文件转换分割"
cat new_*.txt | sed "s/\\x23\\x5F\\x23/\\x80/g" | split -a 3 --additional-suffix=.dat -d -l $CNT --numeric-suffixes=1 - a_10000_"$1"_"$2"_"$3"_00_
rm -f new_*.txt

echo "开始生成校验文件"
for i in $(ls *.dat); do
  if [[ $i==$(ls *.dat | tail -1) ]]; then
     CNT=$(cat $i | wc -l)
  fi
  echo "$i,$(wc -c < $i),$CNT,$1,$(date -d @$(stat -c %Y $i) +%Y%m%d%H%M%S)" | iconv -t GBK | sed "s/\\x2c/\\x80/g" >> a_10000_"$1"_"$2"_"$3"_00.verf
done

echo "开始加密"
# 对所有数据文件进行加密，如失败，则延迟后再尝试
tries=5   # 整体尝试次数
while true; do
  dats=$(ls *.dat 2> /dev/null)
  if [ "${dats}" = "" ]; then break; fi

  for f in ${dats}; do
    echo $f
    openssl enc -aes-128-cbc -in $f -out $f.encrypt -pass pass:UdDA6mEoEPZ7JRxN
#    openssl enc -aes-128-cbc -in $f -out $f.encrypt -pass pass:abcdefghij123456
    rm -f $f
    sleep 3
  done

  tries=$[$tries - 1]
  if [ ${tries} -eq 0 ]; then break; fi
done

echo "重命名"
# 对所有数据文件进行重命名，如失败，则延迟后再尝试
tries=5   # 整体尝试次数
while true; do
  encrypts=$(ls *.dat.encrypt 2> /dev/null)
  if [ "${encrypts}" = "" ]; then break; fi

  for f in ${encrypts}; do
    mv $f ${f:0:37}
    sleep 3
  done

  tries=$[$tries - 1]
  if [ ${tries} -eq 0 ]; then break; fi
done

echo "开始压缩"
# 对所有数据文件进行压缩，如失败，则延迟后再尝试
tries=5   # 整体尝试次数
while true; do
  dats=$(ls *.dat 2> /dev/null)
  if [ "${dats}" = "" ]; then break; fi

  for f in ${dats}; do
    echo $f
    gzip -1f $f
    sleep 3
  done

  tries=$[$tries - 1]
  if [ ${tries} -eq 0 ]; then break; fi
done

echo "流程结束"

# echo "获取转换后的数据文件行数"
# lc_conv=$(zcat *.dat.gz | wc -l)
# echo ${lc_conv}
# 如行数不一致，则退出
# if [ ${lc_orig} -ne ${lc_conv} ]; then
#  exit -1
# fi

#echo "开始生成校验文件"
#for i in $(ls *.dat.encrypt.gz); do
#  if [[ $i==$(ls *.dat.gz | tail -1) ]]; then
#     CNT=$(zcat $i | wc -l)
#  fi
#  echo "$i,$(wc -c < $i),$CNT,$1,$(date -d @$(stat -c %Y $i) +%Y%m%d%H%M%S)" | iconv -t GBK | sed "s/\\x2c/\\x80/g" >> a_10000_"$1"_"$2"_"$3"_00.verf
#done


