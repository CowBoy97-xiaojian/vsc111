ALTER TABLE ham_jituan.ods_client_raw DROP partition(dt='${DT}', hour='${HH}');
ALTER TABLE ham_jituan.ods_client_raw ADD partition(dt='${DT}', hour='${HH}') LOCATION '/flume/dcslogs_jituan/${DT}/${HH}';
