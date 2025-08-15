#!/bin/bash

# 转换后*.dat.gz文件数量
gzcount=$(ls *.dat.gz | wc -l)
echo ${gzcount}
# verf文件行数
verfcount=$(cat *.verf | wc -l) 
echo ${verfcount}

if [ ${gzcount} -ne ${verfcount} ]; then
  exit -1
fi

echo "push start"	
lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3964 sftp://10.252.180.2/incoming/cmccsales_270cm/  -e "mput -c *.dat.gz; mput -c *verf; exit;"
echo "push succeed"
