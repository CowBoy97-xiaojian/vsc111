sh $file $ip $ck_dt $hour
sh /home/udbac/bin/ckout_51006_sql.sh 10.104.81.171 2024-01-20 00

nohup sh -x /home/udbac/bin/ckout_51006_sql.sh '10.104.81.171' '2024-01-20' '00' & \


sh -x test.sh '10.104.81.171' '2024-01-20' '00'