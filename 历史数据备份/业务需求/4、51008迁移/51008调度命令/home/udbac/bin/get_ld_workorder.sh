#!/bin/bash

cd /home/udbac/input/ld_workorder_ck

rm -f *$1*


while true; do
	lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3964 sftp://10.252.180.2/outgoing/cmccsalses_khdwb/ -e "mget *$1*; exit;"
	sleep 3
    if [ -e a_ld_$1_JZYY_XTB2_55201_0?_001.dat -a -e a_ld_$1_JZYY_XTB2_55202_0?_001.dat ]; then break; fi
	sleep 600
done
