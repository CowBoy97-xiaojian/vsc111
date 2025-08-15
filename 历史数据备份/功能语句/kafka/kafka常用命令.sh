kafka-topics --bootstrap-server cache-core-1:9092,cache-core-2:9092,cache-core-3:9092,cache-core-4:9092,cache-core-5:9092 --describe --topic apflow-cm002-200gd

kafka-topics --bootstrap-server cache-core-1:9092,cache-core-2:9092,cache-core-3:9092,cache-core-4:9092,cache-core-5:9092 --describe --topic apflow-cm002-200gd-new

kafka-topics --bootstrap-server cache-core-1:9092,cache-core-2:9092,cache-core-3:9092,cache-core-4:9092,cache-core-5:9092 --create --topic ccc-gd --partitions 5 --replication-factor 2

for i in gio-basicflow-a6381779603b656a gio-basicflow-913e6dc4915d470c; do kafka-topics --bootstrap-server cache-core-1:9092,cache-core-2:9092,cache-core-3:9092,cache-core-4:9092,cache-core-5:9092 --create --topic $i --partitions 5 --replication-factor 5 ;done

kafka-topics --bootstrap-server cache-core-1:9092,cache-core-2:9092,cache-core-3:9092,cache-core-4:9092,cache-core-5:9092 --delete  --topic ccc-gd

kafka-topics --bootstrap-server cache-core-1:9092,cache-core-2:9092,cache-core-3:9092,cache-core-4:9092,cache-core-5:9092 --delete  --topic apflow-cm002-200gd

kafka-topics --bootstrap-server cache-core-1:9092,cache-core-2:9092,cache-core-3:9092,cache-core-4:9092,cache-core-5:9092 --create --topic apflow-cm002-200gd --partitions 5 --replication-factor 2


--producer-property

--property group.id=test

/data1/apps/lib/kafka/bin/kafka-console-consumer.sh --bootstrap-server 10.104.24.244:9092 --group=test --topic test