#!/bin/bash

for i in $(clickhouse-client -h 10.253.248.73 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="select code from ham.ads_rpt_hachi_40001_all group by code FORMAT CSV" | sed "s/\"//g"); do
    echo $i
    clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="alter table ham.ads_rpt_hachi_40001_local on cluster cluster_gio_with_shard drop partition '$i'"
done

