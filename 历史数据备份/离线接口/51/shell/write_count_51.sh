#! /bin/bash


dt=$(date "+%Y-%m-%d %H:%M")

#类型
type=$1

#账期时间
zqdt=$2

#接口文件数量
count=$3

echo "账期,接口,数量,推送时间: ${zqdt}|${type}|${count}|${dt}" >> /home/udbac/file_count.log
