
#!/bin/bash

DTF=$1
DT=$(date -d $DTF +"%Y-%m-%d")


for i in {00..23};do sh /home/udbac/hqls_ck/h5/ck_dcslog_event_hi_gio_51010.sh ${DT} $i;done
for i in {00..23};do sh /home/udbac/hqls_ck/h5/ck_dcslog_event_hi_51010.sh ${DT} $i;done


/home/udbac/bin/ck_bin/ck_getevent_all_51010.sh