
if(instr('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','|')>0,split('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','\\|')[0],'2408:8215:b59:dcb0:7cbe:3233:a622:bbcf') as ip,
if(instr('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','|')>0,split('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','\\|')[1],parse_ip(if(instr('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','|')>0,split('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','\\|')[0],'2408:8215:b59:dcb0:7cbe:3233:a622:bbcf')).prov) as ip_prov,
if(instr('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','|')>0,split('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','\\|')[2],parse_ip('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf').city) as ip_city,


if(instr('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','|')>0,split('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','\\|')[0],'2408:8215:b59:dcb0:7cbe:3233:a622:bbcf') as ip,
if(instr('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','|')>0,split('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','\\|')[1],parse_ip(if(instr('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','|')>0,split('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','\\|')[0],'2408:8215:b59:dcb0:7cbe:3233:a622:bbcf')).prov) as ip_prov,
if(instr('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','|')>0,split('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','\\|')[2],parse_ip(if(instr('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','|')>0,split('2408:8215:b59:dcb0:7cbe:3233:a622:bbcf','\\|')[0],'2408:8215:b59:dcb0:7cbe:3233:a622:bbcf')).city) as ip_city,


for host in seatunnel-{2..10};do scp seatunnel-cluster.sh app@$host:/usr/local/seatunnel-2.3.3/bin; done
for host in seatunnel-{1..10};do hss hdfs@$host "/usr/local/seatunnel-2.3.3/bin/seatunnel-cluster.sh -d"; done

 for host in seatunnel-{1..10};do hss hdfs@$host "/usr/local/seatunnel-2.3.3/bin/stop-seatunnel-cluster.sh"; done