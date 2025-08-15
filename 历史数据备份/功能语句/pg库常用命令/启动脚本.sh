zkServer.sh start
kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties

/usr/local/postgresql/bin/pg_ctl -D /usr/local/postgresql/pgsqldata/ -l /usr/local/postgresql/pgsqldata/logs/pgsql.log start



sh /usr/local/zookeeper/bin/zkServer.sh stop
kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties



INSERT INTO data_white ("WT_COOKIES","BILL_NO","DATA_TIME","WT_EVENT_ID","WT_ET", "WT_CURRENT_URL", "WT_COMPONENT_ID", "WT_MODULE_NO","WT_POINT_POSITION","WT_ENVNAME", "WT_PREPARE1") VALUES (null,null,null,null,null,null,null,null,null,null,null);


INSERT INTO data_white (WT_COOKIES,BILL_NO,DATA_TIME,WT_EVENT_ID,WT_ET,WT_CURRENT_URL, WT_COMPONENT_ID,WT_MODULE_NO,WT_POINT_POSITION,WT_ENVNAME,WT_PREPARE1) VALUES (null,null,null,null,null,null,null,null,null,null,null);

SELECT grantee, table_schema, table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'carnot';