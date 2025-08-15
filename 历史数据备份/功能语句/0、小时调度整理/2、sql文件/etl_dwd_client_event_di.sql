set hive.execution.engine = mr;
set hive.default.fileformat = Orc;
set hive.merge.mapfiles=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set mapred.map.tasks = 12;

--ods到delta层:

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
          --coalesce 从第一个字段往后取第一个不为null的字段
          --str_to_map key=value & key=value 
          str_to_map(coalesce(query, ''), '&', '=') AS query,
          user_agent,
          cookie,
          referer,
          --WT_FPC=id=26a3ebee5bb991d67ca1692019879291:lv=1692313199370:ss=1692313195606
          --id是cookie
          --ss是时间戳   wt_vtvs类似  dcsdat类似  wt_ets类似
          --regexp_extract 正则表达式部分 一个（）代表一个分组  0：取所有 1：索引从1开始
          nullif(regexp_extract(decode_url(cookie), '(id=)([0-9a-zA-Z]+)(:)', 2), '') AS fpc_id,
          nullif(regexp_extract(decode_url(cookie), '(ss=)([0-9]+)', 2), '') AS fpc_ss,
          --dcsid = data_source_id=dcsgswzxehonmgrc8hz5w67g1_9o7q 匹配取
          regexp_extract(dcsid_l, 'dcs[a-z0-9]{22}_([a-z0-9]{4})', 1) AS dcsid,
          dt, hour
   FROM ham_jituan.ods_client_raw
   WHERE 1=1
     AND dt = '${DT}'
     AND hour = '${HH}'
  ) AS tba where daytime rlike '([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})';


utc_date,string,
utc_time,string,
ip,string,
username,string,
host,string,
method,string,
path,string,
query,string,
status,string,
bytes,string,
version,string,
user_agent,string,
cookie,string,
referer,string,
dcsid_l,string,
dt,string,
hour,string,


2023-08-17
23:00:01
124.116.216.237
-
-
POST
/pfm
WT.co=true&WT.av=8.6.0&WT.i_ornt=portrait&WT.mobile=13992793069&WT.mobenc=55187-49257-4991-39208&WT.os=16.3.1&WT.a_nm=中国移动&WT.co_f=41FFE2AA-5F09-4E06-98AC-9F7FF99A52B1&WT.event=pfm_mpxjsapireserr&WT.dm=iPhone15,2&dcsuri=/pfm&WT.pi=MPXpfm&WT.ct=WiFi&WT.sys=custom&WT.cid=&WT.appId=6460749742196442&XY.pfm=VTRrX1lfLPyaItkCjSGJfdrQV7lPuGNCQv2mDEYFEGP5eh508fONctZQf+Wjvm3oskkukMViwuPILPrESYF0lbpgHMe7Pi8WRaDXZd7LcKzDWlXBtX0xucj/F2DlHlGM7hf57qjI0dIyL4W2p7n95Kv2/CqjEmRT6rzIaiZwrwtttf0keugnpTTRSun22lWfH7EQrKlbJL1/GCs7v17adX1uOb5kw8vxDAOIUgxf02q3agpAH2SkDNDgf83z0ZpymkXjeD7hv2KZDLBMsYBcMJGqbezFZFzpbxopmpa59/JPj8HnyrvEQF/yDY7pGnhc0w9/MxA+azn+zU+4byVahkFLTvXQONvEMiyH2W7XscUbCBZ+TbieDmSzjQp37+TvF9M40WTN8JB2ojfw7NdsJrjSwlNhcjt1TBlzZg0QjulS28VUapQvgzBF+MgpRXH0ah5FXre1UAY4f1v1uuNdJJkteKuywDBbbg78VZf/EIzjNqCH2deMfCg0fLE1LOUEENAuHlMRWk/U67iKWfDZDYCadCUbGnXITxWy36fSxCv5SRtY/xfh7/XpQc7Z6F7yDV8Mus+lOwyAxINgeDYL5YVnc722gl3QTK4Lu2yHvctkTqWXh82o8Akkwe4dLJk82/ScjbbWcreNCadpac+m0BHv8XajVSs+r7qVeOgwkifpuB+31rBQowfp4a0CIZZJ3XM3pRqdp/we9rIV+K6NzyCfxlavLWkzT22tuVDjwjaHa3fsK3ZyN573AOk8wPqN6BFJJQdG4zMxzVOhRzDTKalBO3psxbOJR+xNmwyWZpzTgKlEHw6YNWLUzcANozXRJo7DLkD4gcvEhibtZlo6iaBXwtEfPplLoEY2baQPrTtV9VEUqnD7am3DnwguQ53A9nq+QshwL3dtw5geNqgPEJvzn67KupfpcrsfTEtYo4RiJHp8KgGBxFE7BhnkgTvB5iwGPio1eA+l++1lZ2oFXasf06q8rWE699WT4kc+FEp4vNE2ZqPzCvgQ7olWQNCanXEs7eTldQS9i35FhVOUmluk3Foj1ZnK3m+U9ZW1tKZ7gu5K+WHNb4h4Z5Za2LuHEULh+3hTeVIM98HZSZYKZFgxRynqYkP5yPkCzM0JhnfLMGSVtuDuFI3Xl6aI3ygWiK9Nb8ovwTrLwvdvSA+59TBWw+thFoBrKK7S5fkkzINhV3pV9H641eYE7z/ayEURsHvxmanp0aUltUK+jpv9f7zBffGlsKoSmpUKY5CXxEj5aLmCfzLh+LDH8HpYDz2BId0u2E9B/tPbV4TiQ/VjhP8jDq+S0twNLydtktXHExrIXsiw8c5l4vbmdPoboVUQPslqtkBMtLG+1BwftFpbKzXy4Jr7Ffq0DOuZN3gsB7tpfTPa7SPHzGCAwkG57M2q9bn9wmTDuN+bzUz3tChsxrj6mgHlHWfPWKW2jl/hlePPnWSu+uMBU/fmEnn5ScZmqlM2ti2NyToQogEavZlvDHnS/ccS1KaM1S2J0a47FJMRYOLt5lbjaN+7M0NvQXU27miTK3SCnewCYkYIISuAMcLf0jqa7WHSlRwy6QXrJGU=&WT.dl=0&WT.vt_sid=41FFE2AA-5F09-4E06-98AC-9F7FF99A52B1.1692313194972&WT.sr=1179x2556&WT.vt_f_tlh=1692313199818&WT.ets=1692313199820&WT.vtvs=1692313194972&WT.sdk_v=4.0.0&WT.ul=zh&WT.uc=CN&WT.vtid=41FFE2AA-5F09-4E06-98AC-9F7FF99A52B1&WT.a_dc=中国移动&WT.et=pfm&WT.ti=MPXpfm&WT.sdcdat=1692313201437
200
-
HTTP/1.0
WebtrendsClientLibrary/4.0.0+(App_iPhone);(iPhone;+U;+CPU+OS+16.3.1+like+Mac+OS+X;+zh)
WT_FPC=id=26a3ebee5bb991d67ca1692019879291:lv=1692313199370:ss=1692313195606
-
dcsgswzxehonmgrc8hz5w67g1_9o7q