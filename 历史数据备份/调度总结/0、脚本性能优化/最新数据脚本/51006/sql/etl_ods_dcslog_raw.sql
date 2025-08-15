ALTER TABLE ham.ods_dcslog_raw DROP partition(dt='${DT}', hour='${HH}');
ALTER TABLE ham.ods_dcslog_raw ADD partition(dt='${DT}', hour='${HH}') LOCATION '/flume/dcslogs/${DT}/${HH}';
