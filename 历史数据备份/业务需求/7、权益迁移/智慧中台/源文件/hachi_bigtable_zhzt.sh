#!/bin/bash
CNT=800000

cd /home/udbac/output/zhzt/
ago=$(date -d"30 day ago" +%Y%m%d)
echo "清理文件"
rm -rf ${ago}
rm -rf $1
echo "创建文件夹"
mkdir -p $1
cd $1

DT=$(date -d"$1" +%Y-%m-%d)
echo "拉取数据文件"
hdfs dfs -get /user/hive/warehouse/ham_jituan.db/ads_zhzt_amt_di/dt=${DT}/*

date_airflow=$1
file_name="D_029_F30004_A202900002_02912_${date_airflow}_00"
file=${file_name}.txt
CHK_file=${file_name}.chk
date_long=$(date  +%Y%m%d%H%M%S)


echo "增加行号"
touch /home/udbac/output/zhzt/$1/${file}
#for f in $(ls 00* 2> /dev/null); do
#  cat $f | awk '{print NR "€€" $0}' >> ${file}
#  rm $f
#done
cat 00* |  sed 's,#_#,€€,g'| awk '{print NR "€€" $0}' > ${file}
rm 00*


echo "生成校验文件"
CNT=$(cat ${file} | wc -l)
BYTE=$(cat ${file}|wc -c)
echo -e "${file}€€${CHK_file}€€${date_long}€€${BYTE}€€${CNT}€€V1.3.8" > ${CHK_file}