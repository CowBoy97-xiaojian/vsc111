#!/bin/bash

rm -rf /home/udbac/40001/*
cd /home/udbac/40001
hdfs dfs -get hdfs://udbachdp1/user/hive/warehouse/ham.db/ads_rpt_hachi_40001/*

rm -rf /home/udbac/40001_out/*
for i in $(ls)  
  do    code=${i#*=}
	cd /home/udbac/40001/$i 
      zcat *.gz | iconv -c -t GBK > /home/udbac/40001_out/i_10000_${code}_40001_${1}.txt
      cat /home/udbac/40001_out/i_10000_${code}_40001_${1}.txt | awk '{print NR "||" $0}'| sed "s/\\x7c\\x7c/\\x80/g" > /home/udbac/40001_out/i_10000_${code}_40001_${1}.dat
      rm -rf /home/udbac/40001_out/i_10000_${code}_40001_${1}.txt
done

cd /home/udbac/40001_out 
gzip -1 *.dat

for i in $(ls *.dat.gz); do
  if [[ $i==$(ls *.dat.gz | tail -1) ]]; then
     CNT=$(zcat $i | wc -l)
  fi
  echo "$i,$(wc -c < $i),$CNT,$1,$(date -d @$(stat -c %Y $i) +%Y%m%d%H%M%S)" | iconv -t GBK | sed "s/\\x2c/\\x80/g" >> i_10000_40001_${1}.verf 
done

