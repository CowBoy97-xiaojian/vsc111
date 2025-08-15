#! /bin/bash
#当没有文件上传之后结束循环

tries=3
DT=$1
HH=$2
while true;
do
#svc_dcs0cxkozfonmgrs8gfnw57g1_2e4p_sdc_20230818_07-01_shotpot09_99059.log
#当hdfs上传文件的时候文件名后边会带有_COPYING_
        ct=`hdfs dfs -ls /flume/$3/${DT}/${HH}/ | grep "_COPYING_" | wc -l`
        if [ $ct -gt 0 ];then
             sleep 120
        else
            #文件上传的时候会存在空档期，中途出现没有_COPYING_的情况，为了防止数据错误，执行三次检验
             tries=$[$tries - 1]
             if [ ${tries} -eq 0 ]; then break; fi
             sleep 10
        fi
done