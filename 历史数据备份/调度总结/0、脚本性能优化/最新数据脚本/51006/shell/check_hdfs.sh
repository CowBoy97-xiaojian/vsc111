#!/bin/bash


#TBNAME_TAIL=$1
tp=$1


QUERY="delete from dwd_monitor_dh where type='${tp}'and day='$2' and hour='$3' ;"
sqoop eval --connect 'jdbc:postgresql://carnot01:6500/carnot' \
           --username carnot --password carnot@123 \
           --query "$QUERY"
sqoop export --connect 'jdbc:postgresql://carnot01:6500/carnot' \
           --username carnot --password carnot@123 \
           --table "dwd_monitor_dh" \
           --input-null-string '\\N' \
           --fields-terminated-by '|' \
           --export-dir /user/hive/warehouse/ham.db/dwd_monitor_dh_t/tp=${tp}/dt=$2/hour=$3
[udbac@api-core-1 bin]$ cat sqp_exp_carnot_monitor_url.sh 
#!/bin/bash


#TBNAME_TAIL=$1
tp=$1


QUERY="delete from dwd_monitor_url_dh where type='${tp}'and day='$2' and hour='$3' ;"
sqoop eval --connect 'jdbc:postgresql://carnot01:6500/carnot' \
           --username carnot --password carnot@123 \
           --query "$QUERY"
sqoop export --connect 'jdbc:postgresql://carnot01:6500/carnot' \
           --username carnot --password carnot@123 \
           --table "dwd_monitor_url_dh" \
           --input-null-string '\\N' \
           --input-null-non-string '\\N' \
           --fields-terminated-by '|' \
           --export-dir /user/hive/warehouse/ham.db/dwd_monitor_url_dh_t/tp=${tp}/dt=$2/hour=$3
[udbac@api-core-1 bin]$ cat check_hdfs.sh 
#! /bin/bash

tries=3
DT=$1
HH=$2
while true;
do
	ct=`hdfs dfs -ls /flume/$3/${DT}/${HH}/ | grep "_COPYING_" | wc -l`
	if [ $ct -gt 0 ];then
	     sleep 120
	else
             tries=$[$tries - 1]
             if [ ${tries} -eq 0 ]; then break; fi
             sleep 10
        fi
done
