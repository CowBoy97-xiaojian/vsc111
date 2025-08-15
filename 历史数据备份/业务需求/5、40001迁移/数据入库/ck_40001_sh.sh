#!/bin/bash


rm -rf /home/udbac/output_ck/40001_QY/*
cd /home/udbac/output_ck/40001_QY

for i in $(clickhouse-client -h 10.253.248.73 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="select code from ham.ads_rpt_hachi_40001_all group by code FORMAT CSV" | sed "s/\"//g"); do
    #echo $i
    sh ck_get_to_file_40001.sh $i | iconv -c -t GBK | sed "s/\x01/\x80/g"  > i_10000_"$i"_40001_20230420.dat
done

beel
 
gzip -1 *.dat

for i in $(ls *.dat.gz); do
  if [[ $i==$(ls *.dat.gz | tail -1) ]]; then
     CNT=$(zcat $i | wc -l)
  fi
  echo "$i,$(wc -c < $i),$CNT,$1,$(date -d @$(stat -c %Y $i) +%Y%m%d%H%M%S)" | iconv -t GBK | sed "s/\\x2c/\\x80/g" >> i_10000_40001_${1}.verf 
done




