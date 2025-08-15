#!/bin/bash
CNT=800000

cd /home/udbac/output/zhzt_jifei/
ago=$(date -d"30 day ago" +%Y%m%d)
echo "清理文件"
rm -rf ${ago}
rm -rf $1
echo "创建文件夹"
mkdir -p $1
cd $1

DT=$(date -d"$1" +%Y-%m-%d)
echo "拉取数据文件"
hdfs dfs -get /user/hive/warehouse/ham_jituan.db/ods_zhzt_jf_di/dt=${DT}/*

date_airflow=$1
file_name="D_029_F40001_A202900002_02912_${date_airflow}_00_0001"
CHK_file_name="D_029_F40001_A202900002_02912_${date_airflow}_00"
file=${file_name}.txt
CHK_file=${CHK_file_name}.chk

date_long=$(date  +%Y%m%d%H%M%S)

touch /home/udbac/output/zhzt_jifei/$1/${file}
for f in $(ls 00* 2> /dev/null); do
  cat $f | sed 's,#_#,€€,g' >> ${file}
  rm $f
done
gzip -f ${file}

CNT=$(zcat ${file}.gz | wc -l)
echo -e "${file}.gz€€${CHK_file}€€${date_long}€€$(wc -c < ${file}.gz)€€${CNT}€€V1.0.0" > ${CHK_file}
