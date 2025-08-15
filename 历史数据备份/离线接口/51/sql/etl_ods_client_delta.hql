set hive.execution.engine = mr;
set hive.default.fileformat = Orc;
set hive.merge.mapfiles=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set mapred.map.tasks = 12;

INSERT overwrite TABLE ham_jituan.ods_client_delta partition(dt = '${DT}', hour = '${HH}')
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
          regexp_extract(dcsid_l, 'dcs[a-z0-9]{22}_([a-z0-9]{4})', 1) AS dcsid,
          dt, hour
   FROM ham_jituan.ods_client_raw
   WHERE 1=1
     AND dt = '${DT}'
     AND hour = '${HH}'
  ) AS tba where daytime rlike '([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})';

