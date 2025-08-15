#! /bin/bash

dt=$1
name=JZYY_XTB2_51007

#删除天账期下文件
echo "1、删除${dt}账期下文件"

rm -f /data/output/51007_hourly/$dt/*gz
rm -f /data/output/51007_hourly/$dt/*verf

ct=$(ls -d $dt_* | wc -l)
echo "共生成$dt账期$ct个小时数据"
#if [ '23' -ne $ct ]


echo "2、复制博新小时账期数据,聚合小时检验文件数据"

for i in {00..23}
do

#复制小时账期数据
cp /data/output/51007_hourly/$dt/"$dt"_"$i"/*gz /data/output/51007_hourly/$dt/

#聚合小时检验文件数据
cat /data/output/51007_hourly/$dt/"$dt"_"$i"/a_10000_"$dt"_JZYY_XTB2_51007_"$i"_00.verf >> /data/output/51007_hourly/$dt/a_10000_"$dt"_JZYY_XTB2_51007_00.verf

done

echo "3、复制gio小时账期数据,聚合小时检验文件数据"

for i in {00..23}
do

#复制小时账期数据
cp /data/output/51007_gio_hourly/$dt/"$dt"_"$i"/*gz /data/output/51007_hourly/$dt/

#聚合小时检验文件数据
cat /data/output/51007_gio_hourly/$dt/"$dt"_"$i"/a_10000_"$dt"_JZYY_XTB2_51007_gio_"$i"_00.verf >> /data/output/51007_hourly/$dt/a_10000_"$dt"_JZYY_XTB2_51007_00.verf

done

#重命名

echo "4、重命名"
filecount1=1
for i in $(ls *.dat.gz); do
  if [ $filecount1 -lt 10 ];then
     mv $i a_10000_"$dt"_"$name"_00_000"$filecount1".dat.gz
     sed -i "s/$i/a_10000_"$dt"_"$name"_00_000"$filecount1".dat.gz/g" a_10000_"$dt"_"$name"_00.verf
  elif [ $filecount1 -lt 100 ];then
     mv $i a_10000_"$dt"_"$name"_00_00"$filecount1".dat.gz
     sed -i "s/$i/a_10000_"$dt"_"$name"_00_00"$filecount1".dat.gz/g" a_10000_"$dt"_"$name"_00.verf
  elif [ $filecount1 -lt 1000 ];then
     mv $i a_10000_"$dt"_"$name"_00_0"$filecount1".dat.gz
     sed -i "s/$i/a_10000_"$dt"_"$name"_00_0"$filecount1".dat.gz/g" a_10000_"$dt"_"$name"_00.verf
  else
     mv $i a_10000_"$dt"_"$name"_00_"$filecount1".dat.gz
     sed -i "s/$i/a_10000_"$dt"_"$name"_00_"$filecount1".dat.gz/g" a_10000_"$dt"_"$name"_00.verf
  fi
  filecount1=$[$filecount1 + 1]
done