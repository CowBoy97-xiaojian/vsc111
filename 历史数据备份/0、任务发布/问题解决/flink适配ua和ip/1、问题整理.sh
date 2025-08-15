## 重启集群
进168跳板机后执行
for host in seatunnel-{1..10};do hss hdfs@$host "/usr/local/seatunnel-2.3.3/bin/stop-seatunnel-cluster.sh"; done  #停seatunnel集群
for host in seatunnel-{1..10};do hss hdfs@$host "/usr/local/seatunnel-2.3.3/bin/seatunnel-cluster.sh -d"; done    #启动seatunnel集群
hss app@seatunnel-1 #进seatunnel-1 
cd /usr/local/seatunnel-2.3.3
./bin/seatunnel.sh -l   #查看seatunnel状态
sh test.sh # 进hdfs账号
nohup sh  /usr/local/seatunnel-2.3.3/bin/seatunnel.sh  --config /usr/local/seatunnel-2.3.3/config/kafka2hive_text.conf -n hive_newudf > /usr/local/seatunnel-2.3.3/nohup_newudf.log 2>&1 & #重启集群后启动任务

yarnflinkji'qi
10.104.92.247           yarnflink-1
10.104.92.242           yarnflink-2
10.104.92.235           yarnflink-3
10.104.92.229           yarnflink-4
10.104.92.233           yarnflink-5
10.104.92.228           yarnflink-6
10.104.92.241           yarnflink-7
10.104.92.239           yarnflink-8
10.104.92.245           yarnflink-9
10.104.92.232           yarnflink-10
10.104.92.234           yarnflink-11
10.104.92.238           yarnflink-12
10.104.92.240           yarnflink-13
10.104.92.226           yarnflink-14
10.104.92.236           yarnflink-15

for host in yarnflink-{2..15};do scp /data1/flink-1.16.3/conf/ipv4.db  $host:/data1/; done