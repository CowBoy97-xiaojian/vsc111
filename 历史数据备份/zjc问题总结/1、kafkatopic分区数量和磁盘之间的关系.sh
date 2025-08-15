某一个kafka的topic数据量过大，导致数据特别大，某一个磁盘挂了

配置leader副本需默认选举
kafka-configs.sh --bootstrap-server cache-core-new-1:9092 --entity-type topics --entity-name gio-basicflow-b508a809cbbddd0b  --add-config unclean.leader.election.enable=true  --alter