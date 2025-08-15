#!/bin/bash
Njob=24 #任务总数
daytime=$1 #数据日期 2023-05-01
Nproc=$2 #最大并发进程数 12
day=$3 20230501
cd /home/udbac/output_ck/51007_daily/"$daytime"

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

function file_transform(){
time1=$(date "+%Y-%m-%d %H:%M:%S")
echo "开始第"$hour"小时文件切割_当前时间"$time1
zcat 51007_*_"$hour".dat.gz | awk '{print NR "#_#" $0}'| sed "s/\\x23\\x5F\\x23/\\x80/g" |  split -a 4 --additional-suffix=.dat -d -l 800000 -d --numeric-suffixes=1 - 51007_"$hour"_
rm -f 51007_*_"$hour".dat.gz

time2=$(date "+%Y-%m-%d %H:%M:%S")
echo "开始第"$hour"小时文件压缩_当前时间"$time2
dats=$(ls 51007_"$hour"_*.dat 2> /dev/null)
for f in ${dats}; do
    echo "$f.gz,$(wc -c < $f),$(wc -l < $f),$day,$(date +%Y%m%d%H%M%S)" | iconv -t GBK | sed "s/\\x2c/\\x80/g" >> a_10000_"$day"_JZYY_XTB1_51007_00_test.verf
    gzip -f $f
    sleep 3
  done
time3=$(date "+%Y-%m-%d %H:%M:%S")
echo "完成第"$hour"小时文件压缩_当前时间"$time3
}

for ((i=0; i<$Njob; i++)); do
    if [ $i -lt 10 ];then
        hour="0"$i
    else 
        hour=$i
    fi

    file_transform &

    PID=$!

    PushQue $PID

    while [[ $Nrun -ge $Nproc ]]; do          # 如果Nrun大于Nproc，就一直ChkQue

        ChkQue

        sleep 0.1

    done

done

wait

time5=$(date "+%Y-%m-%d %H:%M:%S")
echo "开始生成校验文件_当前时间"$time5
filecount1=1
for i in $(ls *.dat.gz); do
  if [ $filecount1 -lt 10 ];then
     mv $i a_10000_"$day"_JZYY_XTB1_51007_00_000"$filecount1"_test.dat.gz
     sed -i "s/$i/a_10000_"$day"_JZYY_XTB1_51007_00_000"$filecount1"_test.dat.gz/g" a_10000_"$day"_JZYY_XTB1_51007_00_test.verf
  elif [ $filecount1 -lt 100 ];then
     mv $i a_10000_"$day"_JZYY_XTB1_51007_00_00"$filecount1"_test.dat.gz
     sed -i "s/$i/a_10000_"$day"_JZYY_XTB1_51007_00_00"$filecount1"_test.dat.gz/g" a_10000_"$day"_JZYY_XTB1_51007_00_test.verf
  elif [ $filecount1 -lt 1000 ];then
     mv $i a_10000_"$day"_JZYY_XTB1_51007_00_0"$filecount1"_test.dat.gz
     sed -i "s/$i/a_10000_"$day"_JZYY_XTB1_51007_00_0"$filecount1"_test.dat.gz/g" a_10000_"$day"_JZYY_XTB1_51007_00_test.verf
  else
     mv $i a_10000_"$day"_JZYY_XTB1_51007_00_"$filecount1"_test.dat.gz
     sed -i "s/$i/a_10000_"$day"_JZYY_XTB1_51007_00_"$filecount1"_test.dat.gz/g" a_10000_"$day"_JZYY_XTB1_51007_00_test.verf
  fi
  filecount1=$[$filecount1 + 1]
done
time6=$(date "+%Y-%m-%d %H:%M:%S")
echo "完成生成校验文件_当前时间"$time6