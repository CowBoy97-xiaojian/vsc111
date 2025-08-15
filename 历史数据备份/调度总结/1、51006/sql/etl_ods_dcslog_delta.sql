set hive.execution.engine = mr;
set hive.default.fileformat = Orc;
set hive.merge.mapfiles=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set mapred.map.tasks = 12;

INSERT overwrite TABLE ham.ods_dcslog_delta partition(dt = '${DT}', hour = '${HH}')
SELECT daytime,
       ip,
       HOST,
       path,
       query,
       user_agent,
       cookie,
       referer,
       if(query['WT.mobile'] rlike '[0-9]{11}', query['WT.mobile'], NULL) AS mobile,
       coalesce(fpc_id, query['WT.co_f']),
       concat(coalesce(fpc_id, query['WT.co_f']), ':', coalesce(fpc_ss, query['WT.vtvs'])) AS ss_id,
       dcsid
FROM
  (SELECT date_format(from_utc_timestamp(concat(utc_date, ' ', utc_time), 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') AS daytime,
          ip,
          HOST,
          path,
          str_to_map(coalesce(query, ''), '&', '=') AS query,
          user_agent,
          cookie,
          referer,
          nullif(regexp_extract(decode_url(cookie), '(id=)([0-9a-zA-Z]+)(:)', 2), '') AS fpc_id,
          nullif(regexp_extract(decode_url(cookie), '(ss=)([0-9]+)', 2), '') AS fpc_ss,
          regexp_extract(dcsid_l, 'dcs[a-z0-9]{22,24}_([a-z0-9]{4})', 1) AS dcsid,
          dt, hour
   FROM ham.ods_dcslog_raw
   WHERE 1=1
     AND dt = '${DT}'
     AND hour = '${HH}'
  ) AS tba 
WHERE 1=1 
and dcsid in ('3w7x','5i5n','7e9m','5m9b','2r9o','3l2h','4t5g','3s3i','7p1j','3a5k','8z2s','1w7q','9w6z','8f4q','2i1b','6g7b','0d4d','8d2o','8m4a','5d3k','3z9h','4w6p','5f8e','3j1d','9o0p','0o1j','3z7i','9b5g','3r8r','3d2m','9m2x','8o8q','2v6r','8j5v','4z6i','9y7c','8s0b','1g3e','9o4w','7c2d');
