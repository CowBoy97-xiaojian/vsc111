
select count(1)
from ham_jituan.seatunnel_cdpevent
where dt = '2024-02-17'
and hour = '04'
and data_source_id in ('ac36424553dcd3e8','b95440ef47ec01fc','90be4403373b6463','86596eaccd0d746a','8aeb9b26885f3d8b','bdf908bd8e07b82c','938892fec03694af','b00057b79cbf85af','92f9b8b42859ed1c','94bbabc3a9686c5a','ad45b0b4c1ef7446','a9ae56608c62f805','ad3f51110dccb587','ad7c40e8ac8a0983','826f8be0db16938b','9328255238347f80','a930f2f2aee66a7c','b1b4618c1d4fac12','bee33e74dc5e9b38','aa4dbfdc0e193192','af82bebd8421abec','be5412a41f02e47a','8dd990e550265ae5','9c51cb5ab2e5d077','a20bffba73210972','9ead359aaf617556','98e2f7b831f876dd','aebed7d26ca2d38a','ab6b0c4315fa502b','9ed39aa37260081e','806586170173099d','ac34c865ecb163fa','a47b49395334c862','9e488aa27d948855','a1f48d9ff4f42571','b508a809cbbddd0b','ae1f482357600a77')
and event_type='CUSTOM_EVENT'
and str_to_map(regexp_replace(regexp_replace(decode_url(attributes),'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', ''), '^\\{"(.*)"\\}$', '$1'),'\",\"','\":\"')['area_id'] is null 
and str_to_map(regexp_replace(regexp_replace(decode_url(attributes),'\\n+|\\r+|(\\n\\r)+|\\t+|\\|', ''), '^\\{"(.*)"\\}$', '$1'),'\",\"','\":\"')['type'] is null
;

select count(1)
from ham_jituan.seatunnel_cdpevent
where dt = '2024-02-17'
and hour = '04'
and data_source_id in ('ac36424553dcd3e8','b95440ef47ec01fc','90be4403373b6463','86596eaccd0d746a','8aeb9b26885f3d8b','bdf908bd8e07b82c','938892fec03694af','b00057b79cbf85af','92f9b8b42859ed1c','94bbabc3a9686c5a','ad45b0b4c1ef7446','a9ae56608c62f805','ad3f51110dccb587','ad7c40e8ac8a0983','826f8be0db16938b','9328255238347f80','a930f2f2aee66a7c','b1b4618c1d4fac12','bee33e74dc5e9b38','aa4dbfdc0e193192','af82bebd8421abec','be5412a41f02e47a','8dd990e550265ae5','9c51cb5ab2e5d077','a20bffba73210972','9ead359aaf617556','98e2f7b831f876dd','aebed7d26ca2d38a','ab6b0c4315fa502b','9ed39aa37260081e','806586170173099d','ac34c865ecb163fa','a47b49395334c862','9e488aa27d948855','a1f48d9ff4f42571','b508a809cbbddd0b','ae1f482357600a77')
and event_type='CUSTOM_EVENT'
and get_json_object(attributes,'$.area_id') is null
and get_json_object(attributes,'$.type') is null