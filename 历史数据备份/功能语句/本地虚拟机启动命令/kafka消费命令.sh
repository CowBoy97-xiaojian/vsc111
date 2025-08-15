zkServer.sh start
kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties

/usr/local/postgresql/bin/pg_ctl -D /usr/local/postgresql/pgsqldata/ -l /usr/local/postgresql/pgsqldata/logs/pgsql.log start



sh /usr/local/zookeeper/bin/zkServer.sh stop
kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties

kafka-console-consumer.sh --bootstrap-server centos1:9092 --topic test-output --from-beginning

kafka-console-producer.sh --broker-list   centos1:9092 --topic test-input

kafka-topics.sh --zookeeper centos1:2181 --list