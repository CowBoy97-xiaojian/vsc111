
--上线---博新的
clickhouse-client -h 10.104.82.73 -n  --receive_timeout=3600  --query="
select
anonymous_user,
--groupUniqArray(user)[1] as re_user,
--if(match(re_user, '.*==$'),aes_function(re_user),re_user) as mobile
if(match(groupUniqArray(user)[1], '.*==$'),aes_function(groupUniqArray(user)[1]),groupUniqArray(user)[1]) as mobile
from olap.event_all
where toDate(event_time) = '2023-06-28'
and LPAD(toString(toHour(event_time)), 2, '0') = '10'
and data_source_id in ('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w','dcs49gd2jh65d2sj1tsa4rky3fd_7c2d','dcso02mhurci7zvoq6fa18exaf3_3s3i','dcsd38idvz3od4zmyowg1i6d8gx_7p1j','dcsfg7ph7uwat468njtzviizrfg_3a5k','dcs5pdfyfipmbxlfqmmciqw0a8x_8z2s','dcsz3akh3sibqou38s7guavsnr6_1w7q','dcs2skqkfpz7rtvxw6nzu4x4fgp_9w6z','dcsm3es4kqm3wtyo8gpi38qbf0j_8f4q','dcshtucfdltngkqtwvqniz0539u_2i1b','dcs8eooqrfuomftmsq6ewftia1h_6g7b','dcsxqbkupoawb04gcoq57azgdhx_0d4d','dcs6rxy80wuski0lw5qow0zt71c_8d2o','dcss2i3y89j99qo5kxpig0189i2_8m4a','dcs5pf9f1kksnaz4isjpsd7vnjo_5d3k','dcsl3qjy9cra6i10l5nz2rofjhe_3z9h','dcsc76toh4ik0az8db4ukqm84wd_4w6p','dcso43tjakd8vse5zksyuqz507q_5f8e','dcsm7pei65pvjao39ca1ams4nua_3j1d','dcsrsqxecu3ba3yqnifvv0ab9lm_9o0p','dcsppwpcwvuv2e82jv4dhm33v1c_0o1j','dcsjmisz40f127hqfku9tk6ds3n_3z7i','dcsdi06vzmk2pu9t8trixm63ctu_9b5g','dcsstwp3mj9nspzee8rta36eb9o_3r8r','dcsew4pj2u8stb71grgeavoev1w_3d2m','dcs4kes6i4udrbq7nbs1ctvql81_9m2x','dcs717bz69ty9p6a27c37oxohhf_8o8q','dcszcx0kbabmtltjrvlc1770m5q_2v6r','dcsco4d0qquihf4htc4ibo7v0ht_8j5v','dcsgl7axfbazclxywci71r3zqo9_4z6i','dcsh94u50ijasmndx4cym89ffsm_9y7c','dcstwoyw0krzaon00hpbx9hw5au_8s0b','dcs5hjggzsake53s886udkjdyei_1g3e','dcsxx79x7scb3ch36d1admiccz8_3n9u','dcsykjbxwjtpr16z9ky13i6yf2c_6c9w')
and (match(user, '^[0-9]{11}$') or match(user, '.*==$')) 
group by  anonymous_user
;
" > ./20230629.csv

--原始
clickhouse-client -h 10.104.82.73 -n  --receive_timeout=3600  --query="
select
tb3.anonymous_user as anonymous_user,
mobile
from (
select
distinct anonymous_user,
if(match(user, '.*==$'),aes_function(user),user) as mobile,
row_number() over (partition by anonymous_user order by event_time) as ct
from olap.event_all
where toDate(event_time) = '2023-06-28'
and if(length(cast(toHour(toDateTime(event_time)) as String))=1,concat('0',cast(toHour(toDateTime(event_time)) as String)),cast(toHour(toDateTime(event_time)) as String)) = '10'
and data_source_id in ('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w','dcs49gd2jh65d2sj1tsa4rky3fd_7c2d','dcso02mhurci7zvoq6fa18exaf3_3s3i','dcsd38idvz3od4zmyowg1i6d8gx_7p1j','dcsfg7ph7uwat468njtzviizrfg_3a5k','dcs5pdfyfipmbxlfqmmciqw0a8x_8z2s','dcsz3akh3sibqou38s7guavsnr6_1w7q','dcs2skqkfpz7rtvxw6nzu4x4fgp_9w6z','dcsm3es4kqm3wtyo8gpi38qbf0j_8f4q','dcshtucfdltngkqtwvqniz0539u_2i1b','dcs8eooqrfuomftmsq6ewftia1h_6g7b','dcsxqbkupoawb04gcoq57azgdhx_0d4d','dcs6rxy80wuski0lw5qow0zt71c_8d2o','dcss2i3y89j99qo5kxpig0189i2_8m4a','dcs5pf9f1kksnaz4isjpsd7vnjo_5d3k','dcsl3qjy9cra6i10l5nz2rofjhe_3z9h','dcsc76toh4ik0az8db4ukqm84wd_4w6p','dcso43tjakd8vse5zksyuqz507q_5f8e','dcsm7pei65pvjao39ca1ams4nua_3j1d','dcsrsqxecu3ba3yqnifvv0ab9lm_9o0p','dcsppwpcwvuv2e82jv4dhm33v1c_0o1j','dcsjmisz40f127hqfku9tk6ds3n_3z7i','dcsdi06vzmk2pu9t8trixm63ctu_9b5g','dcsstwp3mj9nspzee8rta36eb9o_3r8r','dcsew4pj2u8stb71grgeavoev1w_3d2m','dcs4kes6i4udrbq7nbs1ctvql81_9m2x','dcs717bz69ty9p6a27c37oxohhf_8o8q','dcszcx0kbabmtltjrvlc1770m5q_2v6r','dcsco4d0qquihf4htc4ibo7v0ht_8j5v','dcsgl7axfbazclxywci71r3zqo9_4z6i','dcsh94u50ijasmndx4cym89ffsm_9y7c','dcstwoyw0krzaon00hpbx9hw5au_8s0b','dcs5hjggzsake53s886udkjdyei_1g3e','dcsxx79x7scb3ch36d1admiccz8_3n9u','dcsykjbxwjtpr16z9ky13i6yf2c_6c9w')
and (match(user, '^[0-9]{11}$') or match(user, '.*==$'))
)tb3
where ct = 1
" > ./20230629-over.csv


select formatReadableSize(memory_usage),event_time,query_duration_ms,query_kind,exception,type from system.query_log where toDate(event_time) = '2023-06-29' and toHour(event_time) = '19' and query like '%LPAD%' order by event_time desc;

select formatReadableSize(memory_usage),event_time,query_duration_ms,query_kind,exception,type from system.query_log where toDate(event_time) = '2023-06-29' and toHour(event_time) = '17' and query like '%tb3%' order by event_time desc;

select formatReadableSize(memory_usage),event_time,query_duration_ms,query_kind,exception,type from system.query_log where toDate(event_time) = '2023-06-29' and toHour(event_time) = '18' and query like '%event_hi_dcslog_update_all%' order by event_time desc;



--昨天查询
select formatReadableSize(memory_usage),event_time,query_duration_ms,query_kind,exception,type from system.query_log where toDate(event_time) = '2023-06-28' and toHour(event_time) = '23' and query like '%tb3%';

select formatReadableSize(memory_usage),event_time,query_duration_ms,query_kind,exception,type from system.query_log where toDate(event_time) = '2023-06-29' and toHour(event_time) = '19' and query like '%tb3%' order by event_time desc;


aes_function(if(match(user, '^1[3-9]\d{9}$'), user, ''))

if(match(user, '.*==$'), aes_function(user), user)


内存溢出问题排查结果：
对每个user都调用aes_function函数，优化SQL只对AES加密的user调用aes_function函数，if(match(user, '.*==$'), aes_function(user), user)，内存使用减少为原来的十分之一


数据插入中间表优化：
从olap.event_all插入到中间表webtrends.event_hi_client_update_all，插入某一渠道一小时数据（1570万行）据耗时187s左右，改为启用5个进程在compute-1、3、5、7、9节点上同时从event_local插入event_hi_client_update_local，耗时70s左右