创建topic
	kafka-topics.sh --zookeeper centos1:2181 --create --topic test-input --partitions 5 --replication-factor 1

	