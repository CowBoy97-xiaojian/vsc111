#! /bin/bash

dt=$1
name=JZYY_XTB2_51007
dfmt=$2

start_time=$(date +"%Y-%m-%d %H:%M:%S")
echo "生成文件开始"
echo $start_time

#删除天账期下文件
echo "1、删除${dt}账期下文件"

rm -f /data/output/51007_hourly/$dt/*gz
rm -f /data/output/51007_hourly/$dt/*verf

ct=$(ls -d $dt_* | wc -l)
echo "共生成$dt账期$ct个小时数据"
#if [ '23' -ne $ct ]


echo "2、复制博新小时账期数据,聚合小时检验文件数据"

export_sum=0

for i in {00..23}
do

#复制小时账期数据
cp /data/output/51007_hourly/$dt/"$dt"_"$i"/*gz /data/output/51007_hourly/$dt/

#聚合小时检验文件数据
cat /data/output/51007_hourly/$dt/"$dt"_"$i"/a_10000_"$dt"_JZYY_XTB2_51007_"$i"_00.verf >> /data/output/51007_hourly/$dt/a_10000_"$dt"_JZYY_XTB2_51007_00.verf

#统计小时导出文件行数
export_sum=$[$export_sum + $(cat /data/output/51007_hourly/${dt}/${dt}_${i}/export_${dt}_JZYY_XTB2_51007_${i}.log)]

done

echo "3、复制gio小时账期数据,聚合小时检验文件数据"

for i in {00..23}
do

#复制小时账期数据
cp /data/output/51007_gio_hourly/$dt/"$dt"_"$i"/*gz /data/output/51007_hourly/$dt/

#聚合小时检验文件数据
cat /data/output/51007_gio_hourly/$dt/"$dt"_"$i"/a_10000_"$dt"_JZYY_XTB2_51007_gio_"$i"_00.verf >> /data/output/51007_hourly/$dt/a_10000_"$dt"_JZYY_XTB2_51007_00.verf

#统计小时导出文件行数
export_sum=$[$export_sum + $(cat /data/output/51007_gio_hourly/${dt}/${dt}_${i}/export_${dt}_JZYY_XTB2_51007_gio_${i}.log)]

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



SQL_hive="

select sum(cnt) as cnt
from (
select count(1) as cnt
from ham_jituan.ads_hachi_jzyy_xtb2_51007_gio_dt_hour
where dt='${dfmt}'
union all
select count(1) as cnt
from ham_jituan.ads_hachi_jzyy_xtb2_51007_dt_hour
where dt='${dfmt}'
) tb1
;

"

hive_sum=$(beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac  -e "$SQL_hive")

data_consistency='false'

if [ $((hive_sum/800000 +1 +48)) -ge $filecount1 -a $((hive_sum/800000)) -le $filecount1 ];then 
    data_consistency='true'
fi

end_time=$(date +"%Y-%m-%d %H:%M:%S")
echo "合并文件结束"
echo $end_time


SQL_end="

INSERT OVERWRITE TABLE ham_jituan.data_job_monitoring partition (dt = '${dfmt}',hour = '23',job_name = 'transform_51007_daily')
values ('51007','${start_time}','${end_time}','${filecount1}','${export_sum}','${hive_sum}','${data_consistency}','运行完毕');

"

beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac  -e "$SQL_end"


