--解决song——xy问题


select 
count(1)
from ham_jituan.dwd_client_event_gio_di
where dt='2023-11-20'
--and wt_event='P00000065553'  
and wt_co_f='267a0c710f2ce90f6c41699326990444'
limit 5;


select 
*
from ham_jituan.dwd_client_event_di
where dt='2023-11-20';
--and trmnl_style in ('2e4p','9o7q','2p4f','5q5k')
--and wt_event='P00000065553'  
and wt_co_f='267a0c710f2ce90f6c41699326990444'
limit 5;


select xy,hour
from ham_jituan.dwd_client_event_di
where dt='2023-11-20'
and wt_event='P00000065553'  
and wt_co_f='267a0c710f2ce90f6c41699326990444'
limit 50;

select decode_url(query['WT.XY'])
from ham_jituan.ods_client_delta
where dt='2023-11-20'
and query['WT.co_f'] = '267a0c710f2ce90f6c41699326990444'
and decode_url(query['WT.event']) = 'P00000065553';

select query,decode_url(parsedata_xy_ys2(query)),decode_url(query['WT.XY'])
from ham_jituan.ods_client_delta
where dt='2023-11-20'
--and dcsid in ('9o7q','2e4p')
limit 10;



select decodeURLComponent(attributes['WT_XY']) from 
olap.event_all
where toDate(event_time)='2023-11-20'
and data_source_id = 'dcs47gbrugonmg3u1x8njabg1_2p4f'
and  attributes['WT_event']='P00000065553' and attributes['WT_co_f']='267a0c710f2ce90f6c41699326990444' limit 5
format CSV;


create function parseData_XY_YS2 as 'com.udf.ExtractJsonUDFYS' using jar 'hdfs://udbachdp1/hive-udf/hive-xy-wt-ys.jar';

com.udf.ExtractJsonUDFYS


--postsql
psql "host=10.253.182.68 port=6500 user=clsuser  password=cls@123 dbname=cls_prod" << EOF
\COPY (
    select to_char(now(),'yyyyMMddHH24missms'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss') - INTERVAL '10 min','yyyyMMddHH24miss'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss'),'yyyyMMddHH24miss'),
    'KHDJY',
    'OTHER',
    '999999',
    'TYRZ',
    case when 
    page='我的' then '01' when 
    page='探索' then '02' when 
    page='话费余额' then '03' when 
    page='首页' then '04' 
    when page='商城' then '05'          
    when page='套餐余量' then '06' 
    when page='流量查询' then '07' 
    when page='已订业务' then '08' 
    when page='账单查询' then '09' else '' end as codes,
    'TYRZ',
    '999999',
    '000',
    'PM-KHDJY-OTHER-01-003-000',
    uv,'','','','','' 
    from cls_quality_monitor_ind 
    where window_end='$time' 

    union all 
    select 
    to_char(now(),'yyyyMMddHH24missms'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss') - INTERVAL '10 min','yyyyMMddHH24miss'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss'),'yyyyMMddHH24miss'),
    'KHDJY',
    'OTHER',
    '999999',
    'TYRZ',
    case when 
    page='我的' then '01' when 
    page='探索' then '02' when 
    page='话费余额' then '03' when 
    page='首页' then '04' when 
    page='商城' then '05' when 
    page='套餐余量' then '06' when 
    page='流量查询' then '07' when 
    page='已订业务' then '08' when 
    page='账单查询' then '09' else '' end as codes,
    'TYRZ',
    '999999',
    '000',
    'PM-KHDJY-OTHER-07-003-000',
    round(cast(load_avg as numeric)/cast(1000 as numeric),2)
    ,'','','','','' 
    from cls_quality_monitor_ind 
    where window_end='$time' 

    union all 
    select to_char(now(),'yyyyMMddHH24missms'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss') - INTERVAL '10 min','yyyyMMddHH24miss'),
    to_char(to_timestamp(window_end,'yyyy-mm-dd hh24:mi:ss'),'yyyyMMddHH24miss'),
    'KHDJY',
    'OTHER',
    '999999',
    'TYRZ',
    case when 
    page='我的' then '01' when 
    page='探索' then '02' when 
    page='话费余额' then '03' when 
    page='首页' then '04' when 
    page='商城' then '05' when 
    page='套餐余量' then '06' when 
    page='流量查询' then '07' when 
    page='已订业务' then '08' when 
    page='账单查询' then '09' else '' end as codes,
    'TYRZ',
    '999999',
    '000',
    'PM-KHDJY-OTHER-07-002-000',
    round(cast(access_avg as numeric)/cast(1000 as numeric),2) 
    ,'','','','','' 
    from cls_quality_monitor_ind 
    where window_end='$time') 
    
    TO '/home/udbac/zhil.csv' (format csv, delimiter '|')
\q
EOF

cls_prod
--查看建表sql
pg_dump -h pg01 -p 6500  -U  postgres postgres -s cls_quality_monitor_ind>activiti.sql 

SELECT pg_get_create_table_statement('cls_quality_monitor_ind') AS create_table_statement
FROM information_schema.tables
WHERE table_catalog = 'cls_prod'
  AND table_schema = 'public'
  AND table_name = 'cls_quality_monitor_ind';

  create table (
    [字段名1] [类型1] <references 关联表名(关联的字段名)>;
    ,[字段名2] [类型2],…<,primary key (字段名m,字段名n,…)>;);

 window_end | character varying(20) | not null
 page       | character varying(20) | not null
 uv         | bigint                | 
 load_avg   | bigint                | 
 access_avg | bigint                | 

CREATE TABLE cls_quality_monitor_ind_test  
( 
window_end character varying(20),
page character varying(20),
uv bigint,
load_avg bigint,
access_avg bigint
);



{""WT_es"":""https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html"",
""WT_ti"":""中国移动手机营业厅首页"",
""WT_et"":""imp"",
""WT_envName"":"" 我的卡券"",
""WT_event"":""P00000054139"",
""WT_next_url"":""https://touch.10086.cn/i/mobile/mycoupons.html?e=99&sign=ffffffe0&channelId=P00000054139&yx=1069613149"",
""WT_markId"":""1069613149"",
""WT_serial_no"":""
""}

{
	'WT_loginProvince': '771',
	'WT_av': 'APP_android_8.6.0',
	'$path': '/cmcc-app/app-pages/home.html',
	'P00000054139': '{"WT_es":"https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html","WT_ti":"中国移动手机营业厅首页","WT_et":"imp","WT_envName":" 我的卡券","WT_event":"P00000054139","WT_next_url":"https://touch.10086.cn/i/mobile/mycoupons.html?e=99&sign=ffffffe0&channelId=P00000054139&yx=1069613149","WT_markId":"1069613149","WT_serial_no":""}',
	'type': 'once',
	'area_id': '20230710004_97',
	'P00000054142': '{"WT_es":"https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html","WT_ti":"中国移动手机营业厅首页","WT_et":"imp","WT_envName":" 签到领奖","WT_event":"P00000054142","WT_next_url":"https://app.10086.cn/activity/transferXcx/index.html?platform=cmcc&appId=6460749742196442&path=pages%2Fqwhdmark%2Fviews%2Fhome%2Findex&touch_id=JTST_P00000054142&yx=1069613210&channelId=P00000054142","WT_markId":"1069613210","WT_serial_no":""}',
	'WT_prov': '771',
	'P00000054140': '{"WT_es":"https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html","WT_ti":"中国移动手机营业厅首页","WT_et":"imp","WT_envName":"充值交费","WT_event":"P00000054140","WT_next_url":"CN00052","WT_markId":"1069613075","WT_serial_no":""}',
	'P00000054141': '{"WT_es":"https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html","WT_ti":"中国移动手机营业厅首页","WT_et":"imp","WT_envName":"账单查询","WT_event":"P00000054141","WT_next_url":"https://h.app.coc.10086.cn/cmcc-app/personalBill/phoneBillsNew.html?channelId=P00000054141&yx=1069613164","WT_markId":"1069613164","WT_serial_no":""}',
	'WT_city': '0775',
	'WT_clientID': 'fNFKCyw/z32/NRFivTRGpSDDMfwjSvvGkxA5YAP1rOP9MU1H1f8umZAMGpQahyt3qj51VegEY9xohxSDvp6K7J4RL5kJr2pr3cGmVh9fjtQXj66oCXD+8FCnQd2MItyuHtYvvEXQzAs=',
	'WT_aav': '8.6.0',
	'$title': '中国移动手机营业厅首页',
	'WT_userBrand': '03',
	'WT_loginCity': '0775',
	'WT_cid': 'fNFKCyw/z32/NRFivTRGpSDDMfwjSvvGkxA5YAP1rOP9MU1H1f8umZAMGpQahyt3qj51VegEY9xohxSDvp6K7J4RL5kJr2pr3cGmVh9fjtQXj66oCXD+8FCnQd2MItyuHtYvvEXQzAs='
}


""P00000054139"":
""{
  \""WT_es\"":\""https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html\"",
  \""WT_ti\"":\""中国移动手机营业厅首页\"",
  \""WT_et\"":\""imp\"",
  \""WT_envName\"":\"" 我的卡券\"",
  \""WT_event\"":\""P00000054139\"",
  \""WT_next_url\"":\""https://touch.10086.cn/i/mobile/mycoupons.html?e=99&sign=ffffffe0&channelId=P00000054139&yx=1069613149\"",\""WT_markId\"":\""1069613149\"",
  \""WT_serial_no\"":\"" \""
  }""


select *
from olap.event_all
WHERE toDate(event_time) = '2023-11-24'
and toHour(event_time) = 12 
and attributes['P00000054139'] is not null
and  JSONExtractRaw(attributes['P00000054139'],'WT_event') ='P00000054139'
--and attributes['P00000054139'] like '%copyData%'
limit 5
format CSVWithNames;


[app@application-1 clickhousedata-data-bak]$ cat clickhousedata-data-drop.sh
#!/bin/bash

# 打印开始时间
echo "Script started at $(date)"

# 跳板主机
jump_host="application-1"

# 远程主机列表
remote_hosts=("compute-10")

# 循环遍历远程主机
for remote_host in "${remote_hosts[@]}"; do
    echo "Connecting to $remote_host..."
    
    # 循环遍历日期
    for date in {2023112620231020}; do
        # 命令
        command_to_execute="echo \"alter table olap.event_local on cluster cluster_gio_with_shard drop partition '$date';\" | clickhouse-client"
        
        # 执行跳转到远程主机，并将命令结果保存到变量中
        result=$(ssh -q -J $jump_host $remote_host "$command_to_execute")

        # 打印命令结果
        echo "Command result on $remote_host:"
        echo "$result"

        # 检查命令返回值
        if [ $? -eq 0 ]; then
            echo "Command executed successfully."
        else
            echo "Command execution failed."
        fi

        echo "--------------------------------------"
    done

    echo "All commands executed for $remote_host."
done

# 打印结束时间
echo "Script finished at $(date)"


clickhouse-client -h 10.104.82.67 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="alter table webtrends.evevent_hi_client_imp_local on cluster cluster_gio_with_shard drop partition ('2023-11-26','$1')"


alter table webtrends.event_51010_tmp_local on cluster cluster_gio_with_shard drop partition ('2023-07-19','02')

https://h.app.coc.10086.cn/cmcc-app/app-papes/home.html?papeId=1507252716853313536&channelId=P0000003630&sellerId=1523034HD1701300008

select *
from olap.event_all
where
user = '15722744203' 
and event_time = '1700900584476' 
and session='43abbb4d-0d04-407c-9500-7535767f4eb6'
format CSVWithNames;



{'WT_loginProvince':'250',
'P00000025147':'{"WT_et":"imp","WT_event":"P00000025147","WT_envname":"当月热销","WT_markId":"1046273068","WT_next_url":"https://wap.js.10086.cn/vw/navbar/YHHD?ch=7x&channelId=P00000025147&yx=1046273068"}',
'WT_av':'APP_ios_9.3.0',
'$path':'/cmcc-app/homePlusNew/index.html',
'P00000025148':'{"WT_et":"imp","WT_event":"P00000025148","WT_envname":"充值交费","WT_markId":"1059030002","WT_next_url":"CN00052"}',
'P00000025149':'{"WT_et":"imp","WT_event":"P00000025149","WT_envname":"福利","WT_markId":"1046273030","WT_next_url":"https://wap.js.10086.cn/vw/navbar/ppzq?ch=7x&channelId=P00000025149&yx=1046273030"}',
'type':'once',
'area_id':'20221213004',
'WT_prov':'250',
'WT_city':'0519',
'WT_clientID':'AF8BF7F48BC54AAA9EE61C3D37DECC04',
'WT_aav':'9.3.0',
'$title':'中国移动手机营业厅首页PLUS2.0',
'WT_userBrand':'09',
'WT_loginCity':'0519',
'P00000025150':'{"WT_et":"imp","WT_event":"P00000025150","WT_envname":"查询服务","WT_markId":"1046273086","WT_next_url":"https://wap.js.10086.cn/vw/navbar/CXFWAPP?ch=7x&channelId=P00000025150&yx=1046273086"}','WT_cid':'d16a37c5e12d395c1e055232b908599a8e7a050743b31b271f0931510760eda8'}

{
	'WT_loginProvince': '250',
	'P00000025147': '{"WT_et":"imp","WT_event":"P00000025147","WT_envname":"当月热销","WT_markId":"1046273068","WT_next_url":"https://wap.js.10086.cn/vw/navbar/YHHD?ch=7x&channelId=P00000025147&yx=1046273068"}',
	'WT_av': 'APP_ios_9.3.0',
	'$path': '/cmcc-app/homePlusNew/index.html',
	'P00000025148': '{"WT_et":"imp","WT_event":"P00000025148","WT_envname":"充值交费","WT_markId":"1059030002","WT_next_url":"CN00052"}',
	'P00000025149': '{"WT_et":"imp","WT_event":"P00000025149","WT_envname":"福利","WT_markId":"1046273030","WT_next_url":"https://wap.js.10086.cn/vw/navbar/ppzq?ch=7x&channelId=P00000025149&yx=1046273030"}',
	'type': 'once',
	'area_id': '20221213004',
	'WT_prov': '250',
	'WT_city': '0519',
	'WT_clientID': 'AF8BF7F48BC54AAA9EE61C3D37DECC04',
	'WT_aav': '9.3.0',
	'$title': '中国移动手机营业厅首页PLUS2.0',
	'WT_userBrand': '09',
	'WT_loginCity': '0519',
	'P00000025150': '{"WT_et":"imp","WT_event":"P00000025150","WT_envname":"查询服务","WT_markId":"1046273086","WT_next_url":"https://wap.js.10086.cn/vw/navbar/CXFWAPP?ch=7x&channelId=P00000025150&yx=1046273086"}',
	'WT_cid': 'd16a37c5e12d395c1e055232b908599a8e7a050743b31b271f0931510760eda8'
}

{"aap1":"ppa1","p00000100":"{\"a\":\"a\",\"b\":\"b\",\"c\":\"\"}"}




select * from 
olap.event_all
where  
event_time='1700904022412'
and data_source_id='b95440ef47ec01fc'
and user = '13937452501'
--and session='7a24ed9c-134a-4fe9-9afe-f3353229ed2a'
--and  anonymous_user ='8027d811-4bf7-4a28-aedc-05a73019a4d1'
and session='dd85a902-0994-4f03-90a6-d089435fb74f'
--and  anonymous_user ='2af5d6f1-fd77-4f92-bfeb-3c8e laa1 469a'
--2af5d6f1-fd77-4f92-bfeb-3c8e 1aa1 469a
format CSVWithNames;
;


"project_id","event_key","event_time","event_id","event_type","client_time","anonymous_user","user","user_key","gio_id","merged_gio_id","session","attributes","package","platform","referrer_domain","utm_source","utm_medium","utm_campaign","utm_term","utm_content","traffic_source","ads_id","key_word","country_code","country_name","region","city","browser","browser_version","os","os_version","client_version","channel","device_brand","device_model","device_type","device_orientation","resolution","language","referrer_type","account_id","domain","ip","user_agent","sdk_version","location_latitude","location_longitude","bot_id","data_source_id","esid","origin_data","trace_info","old_gio_id"
1,"imp","2023-11-25 17:20:22.412","40c3a26fc3a6b5fb3e2765bde510cd34","custom_event","2023-11-25 17:20:20.274","2af5d6f1-fd77-4f92-bfeb-3c8e1aa1469a","13937452501","$basic_userId",456,456,"dd85a902-0994-4f03-90a6-d089435fb74f","{'WT_loginProvince':'371','WT_av':'APP_android_9.3.0','$path':'/cmcc-app/app-pages/home.html','P00000054235':'{""WT_es"":""https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html"",""WT_ti"":""中国移动手机营业厅首页"",""WT_et"":""imp"",""WT_envName"":""中国移动在售资费公示"",""WT_event"":""P00000054235"",""WT_next_url"":""https://h.app.coc.10086.cn/cmcc-app/uni-pages/tariffZone.html?channelId=P00000001113"",""WT_markId"":""1069613186"",""WT_serial_no"":""""}','type':'once','area_id':'20230710016_6','P00000054233':'{""WT_es"":""https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html"",""WT_ti"":""中国移动手机营业厅首页"",""WT_et"":""imp"",""WT_envName"":""用户满意度调研"",""WT_event"":""P00000054233"",""WT_next_url"":""https://t.hefen.10086.cn/html5/views/survey/survey_detail.html?id=11694&channel=diaoyan&channelId=P00000054233&yx=1088968002"",""WT_markId"":""1088968002"",""WT_serial_no"":""""}','WT_prov':'371','WT_city':'0374','WT_clientID':'eTQPppnEl7Zgtc4xwneJborWTEfqW7CPhN/ZZNLzQxIk9FXREBt+IPFJzHDCn00pj+WEUe2B9VIEymuWrsKOVOpmLUWaY2tQkC3GaamiiY8wDB8ehMcxHVizH+PQZXCMCXihJBBJLVQ=','WT_aav':'9.3.0','$title':'中国移动手机营业厅首页','WT_userBrand':'09','WT_loginCity':'0374','WT_cid':'eTQPppnEl7Zgtc4xwneJborWTEfqW7CPhN/ZZNLzQxIk9FXREBt+IPFJzHDCn00pj+WEUe2B9VIEymuWrsKOVOpmLUWaY2tQkC3GaamiiY8wDB8ehMcxHVizH+PQZXCMCXihJBBJLVQ='}",\N,"Web","h.app.coc.10086.cn",\N,\N,\N,\N,\N,\N,\N,\N,\N,\N,\N,\N,"Chrome Mobile WebView","Chrome Mobile WebView 88.0.4324","Android","Android 10","1.0.0",\N,"Huawei","Huawei 荣耀Play5T",\N,"PORTRAIT","800*360","zh-hans",\N,"9e4e5fa7244c6b6e","h.app.coc.10086.cn","2409:8947:5a46:1d6:1:0:43fc:46ab","Mozilla/5.0 (Linux; Android 10; KOZ-AL40 Build/HONORKOZ-AL40; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/88.0.4324.93 Mobile Safari/537.36 leadeon/9.3.0/CMCCIT","3.8.6-rc.2",\N,\N,\N,"b95440ef47ec01fc",6,"{""anonymousId"":""2af5d6f1-fd77-4f92-bfeb-3c8e1aa1469a"",""projectKey"":""9e4e5fa7244c6b6e"",""userId"":""13937452501"",""timestamp"":""1700904020274"",""esid"":6,""session"":""dd85a902-0994-4f03-90a6-d089435fb74f"",""eventKey"":""imp"",""type"":""CUSTOM_EVENT"",""version"":""3.8.6-rc.2"",""ip"":""2409:8947:5a46:1d6:1:0:43fc:46ab"",""context"":{""app"":{""version"":""1.0.0"",""userAgent"":""Mozilla/5.0 (Linux; Android 10; KOZ-AL40 Build/HONORKOZ-AL40; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/88.0.4324.93 Mobile Safari/537.36 leadeon/9.3.0/CMCCIT""},""device"":{""platform"":""web""},""screen"":{""width"":360,""height"":800},""operationSystem"":{""name"":""web""},""page"":{""domain"":""h.app.coc.10086.cn"",""path"":""/cmcc-app/app-pages/home.html"",""title"":""中国移动手机营业厅首页""},""referrer"":{""from"":""https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html""},""location"":{},""locale"":{""language"":""zh-CN""}},""attributes"":{""WT_cid"":""eTQPppnEl7Zgtc4xwneJborWTEfqW7CPhN/ZZNLzQxIk9FXREBt+IPFJzHDCn00pj+WEUe2B9VIEymuWrsKOVOpmLUWaY2tQkC3GaamiiY8wDB8ehMcxHVizH+PQZXCMCXihJBBJLVQ="",""WT_clientID"":""eTQPppnEl7Zgtc4xwneJborWTEfqW7CPhN/ZZNLzQxIk9FXREBt+IPFJzHDCn00pj+WEUe2B9VIEymuWrsKOVOpmLUWaY2tQkC3GaamiiY8wDB8ehMcxHVizH+PQZXCMCXihJBBJLVQ="",""WT_userBrand"":""09"",""WT_loginProvince"":""371"",""WT_loginCity"":""0374"",""WT_prov"":""371"",""WT_city"":""0374"",""WT_av"":""APP_android_9.3.0"",""WT_aav"":""9.3.0"",""area_id"":""20230710016_6"",""type"":""once"",""P00000054233"":""{\""WT_es\"":\""https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html\"",\""WT_ti\"":\""中国移动手机营业厅首页\"",\""WT_et\"":\""imp\"",\""WT_envName\"":\""用户满意度调研\"",\""WT_event\"":\""P00000054233\"",\""WT_next_url\"":\""https://t.hefen.10086.cn/html5/views/survey/survey_detail.html?id=11694&channel=diaoyan&channelId=P00000054233&yx=1088968002\"",\""WT_markId\"":\""1088968002\"",\""WT_serial_no\"":\""\""}"",""P00000054235"":""{\""WT_es\"":\""https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html\"",\""WT_ti\"":\""中国移动手机营业厅首页\"",\""WT_et\"":\""imp\"",\""WT_envName\"":\""中国移动在售资费公示\"",\""WT_event\"":\""P00000054235\"",\""WT_next_url\"":\""https://h.app.coc.10086.cn/cmcc-app/uni-pages/tariffZone.html?channelId=P00000001113\"",\""WT_markId\"":\""1069613186\"",\""WT_serial_no\"":\""\""}""},""sendTime"":""1700904022412"",""dataSourceId"":""b95440ef47ec01fc"",""item"":{},""userKey"":""$basic_userId""}","{}",\N



6623502731473049

6623502731473049

appId=6623502731473049


{"dcsid":"9o7q","WT.ct":"WiFi","WT.os":"16.5","WT.cid":"bbbd984af61fa3d498c30f333bcfe1bdccd9f873cd9c3b5b1c5ae16f0a9cf069","WT.av":"9.5.0","wt_event":"pfm_mpxdnloaded","WT.dm":"iPhone12,1","wt_appId":"6623502731473049","trmnl_style":"IOS","XY.pfm":"INK/fwIvXzoD4MBLO94ledEtASTAIwmqUT5Y+pad7ubnkoV2qyxWe7n8nd9bB6fFfmmIQmVddmewfb9o+3IFl+9lByNvBzFbetJ6myLEiZiHbxqeREJz96BQ/8+JYSr4CQW7cw8ozOAxKTe69huHYRHDl2IR5Ajw1toq87kbI1CNoRVcFIUaGeAkXUyncNDo4mhKz7UY8BSK8k3cD0mjW7swqkwibQfIkpqq5Ui3BkyZk2JbvdoY523l7Xvh/gfjRAzI0jTZJYjJP8gamBS6FXiI5TNpwqs9The+c5ygZiEWr6l2YG9BC21DLmKxhP2ag0xIPUVpxe2ag6GfYHDN9qXgGA1kfXIrz5VeyURgNw2U6FEPTPZwbanwFjhP51KHmQmUYzbJXVniWiNPvvfQVoFz0bvouh8RtmsCUqd5KRJb+Y29nRYfKcJBZsQDr4wgFCygBR7FPZ67DZsCWY2cWPeY4Ogwi1ZlG/lVIZsRv5A="}
{"dcsid":"9o7q","WT.ct":"WiFi","WT.os":"16.5","WT.cid":"bbbd984af61fa3d498c30f333bcfe1bdccd9f873cd9c3b5b1c5ae16f0a9cf069","WT.av":"9.5.0","wt_event":"pfm_mpxed","WT.dm":"iPhone12,1","wt_appId":"6623502731473049","trmnl_style":"IOS","XY.pfm":"INK/fwIvXzoD4MBLO94ledEtASTAIwmqUT5Y+pad7ubnkoV2qyxWe7n8nd9bB6fFfmmIQmVddmewfb9o+3IFl+9lByNvBzFbetJ6myLEiZiHbxqeREJz96BQ/8+JYSr4CQW7cw8ozOAxKTe69huHYRHDl2IR5Ajw1toq87kbI1CNoRVcFIUaGeAkXUyncNDo4mhKz7UY8BSK8k3cD0mjW7swqkwibQfIkpqq5Ui3BkyZk2JbvdoY523l7Xvh/gfjRAzI0jTZJYjJP8gamBS6FXiI5TNpwqs9The+c5ygZiEWr6l2YG9BC21DLmKxhP2ag0xIPUVpxe2ag6GfYHDN9qXgGA1kfXIrz5VeyURgNw2U6FEPTPZwbanwFjhP51KHmQmUYzbJXVniWiNPvvfQVmt833p4tY/9fmNkOpVWX84LhC6/APREl9IcCaq3ZaUlDaZbyr3DzPh7X/uiOQzHNw=="}
{"dcsid":"9o7q","WT.ct":"WiFi","WT.os":"16.5","WT.cid":"bbbd984af61fa3d498c30f333bcfe1bdccd9f873cd9c3b5b1c5ae16f0a9cf069","WT.av":"9.5.0","wt_event":"pfm_mpxdnloadedsum","WT.dm":"iPhone12,1","wt_appId":"6623502731473049","trmnl_style":"IOS","XY.pfm":"INK/fwIvXzoD4MBLO94ledEtASTAIwmqUT5Y+pad7ubnkoV2qyxWe7n8nd9bB6fFfmmIQmVddmewfb9o+3IFl+9lByNvBzFbetJ6myLEiZiHbxqeREJz96BQ/8+JYSr4CQW7cw8ozOAxKTe69huHYRHDl2IR5Ajw1toq87kbI1CNoRVcFIUaGeAkXUyncNDo4mhKz7UY8BSK8k3cD0mjW7swqkwibQfIkpqq5Ui3BkyZk2JbvdoY523l7Xvh/gfjRAzI0jTZJYjJP8gamBS6FXiI5TNpwqs9The+c5ygZiEWr6l2YG9BC21DLmKxhP2ag0xIPUVpxe2ag6GfYHDN9qXgGA1kfXIrz5VeyURgNw2U6FEPTPZwbanwFjhP51KHmQmUYzbJXVniWiNPvvfQVjGqeRge3Mnw2o6ttkqVYQqDHGahhVlQmbQKBS+L0HV9J78r/eKsmdCYOXs9E609hlpApixZWJ5kiLw7+g1J55M="}
{"dcsid":"b508a809cbbddd0b","WT.cid":"bbbd984af61fa3d498c30f333bcfe1bdccd9f873cd9c3b5b1c5ae16f0a9cf069","WT.av":"APP_ios_v9.5.0","wt_event":"pfm_mpxed","wt_appId":"6623502731473049","trmnl_style":"IOS","XY.pfm":"INK/fwIvXzoD4MBLO94ledEtASTAIwmqUT5Y+pad7ubnkoV2qyxWe7n8nd9bB6fFfmmIQmVddmewfb9o+3IFl+9lByNvBzFbetJ6myLEiZiHbxqeREJz96BQ/8+JYSr4CQW7cw8ozOAxKTe69huHYRHDl2IR5Ajw1toq87kbI1CNoRVcFIUaGeAkXUyncNDo4mhKz7UY8BSK8k3cD0mjW7swqkwibQfIkpqq5Ui3BkyZk2JbvdoY523l7Xvh/gfjRAzI0jTZJYjJP8gamBS6FXiI5TNpwqs9The+c5ygZiEWr6l2YG9BC21DLmKxhP2ag0xIPUVpxe2ag6GfYHDN9qXgGA1kfXIrz5VeyURgNw2U6FEPTPZwbanwFjhP51KHmQmUYzbJXVniWiNPvvfQVmt833p4tY/9fmNkOpVWX84LhC6/APREl9IcCaq3ZaUlDaZbyr3DzPh7X/uiOQzHNw=="}
{"dcsid":"b508a809cbbddd0b","WT.cid":"bbbd984af61fa3d498c30f333bcfe1bdccd9f873cd9c3b5b1c5ae16f0a9cf069","WT.av":"APP_ios_v9.5.0","wt_event":"pfm_mpxdnloadedsum","wt_appId":"6623502731473049","trmnl_style":"IOS","XY.pfm":"INK/fwIvXzoD4MBLO94ledEtASTAIwmqUT5Y+pad7ubnkoV2qyxWe7n8nd9bB6fFfmmIQmVddmewfb9o+3IFl+9lByNvBzFbetJ6myLEiZiHbxqeREJz96BQ/8+JYSr4CQW7cw8ozOAxKTe69huHYRHDl2IR5Ajw1toq87kbI1CNoRVcFIUaGeAkXUyncNDo4mhKz7UY8BSK8k3cD0mjW7swqkwibQfIkpqq5Ui3BkyZk2JbvdoY523l7Xvh/gfjRAzI0jTZJYjJP8gamBS6FXiI5TNpwqs9The+c5ygZiEWr6l2YG9BC21DLmKxhP2ag0xIPUVpxe2ag6GfYHDN9qXgGA1kfXIrz5VeyURgNw2U6FEPTPZwbanwFjhP51KHmQmUYzbJXVniWiNPvvfQVjGqeRge3Mnw2o6ttkqVYQqDHGahhVlQmbQKBS+L0HV9J78r/eKsmdCYOXs9E609hlpApixZWJ5kiLw7+g1J55M="}



{"dcsid":"a1f48d9ff4f42571","wt_event":"pfm_mpxjsapireserr","wt_appId":"2936119240795401","trmnl_style":"ANDROID","XY.pfm":"uLgPk7DkTPg06MLgLVp8sBSPu+HOJQLIz2NIXoNdZC1Wfu+eTxnxLCnmD4ShAAABkb8vzjdZmp7wdXeQJRc82Y/QUTqmLOjOMpIF15ZH/SD77efsCqCQnlM5/eTRqpSufSTDvRSwFgjBhn43NH9RNvd1hQ3OAXIT/e/O7qxsR3LZvGtZvYOyhB6sGIZYWI2tr7BJHdDl10VggO3U0/fI/NclbrgPIIrN+xKYQ291dVR9wXwcngKAYgVXg05bWDSeqh49/9eNOQz+sFQswiz1RTb+mFtcfuti4Bcoe0Mw4C/pa1wQ3pjqNzuFl/rQFl5XrYhRMBhxyyAGmvkcjasATcKv8ud6ovv9xqgrntirFLVCH5Vio55Hmqdr+nyzibzyx6/kvsw0KrJfI1etf2OomxVe4AembZbFlJgcbbmVSHrmA9LhezJa/1gXWzY2ehrFrAW67z2kxjkQazvMjGCIIkbKMPfvg4vwoPV17iZYUGQ4nwY4uE56czcpBVwLZYWhcBNn/NItVSah4b74S8LwUuadAajdmIz7Lp2UTPqtE7TeoWbvoChJgkArm606lfTZVM2+e+4jkcgfYFL4RK8XTFnVO1Afk7ijnkTf4cos5Zo="}



select dt,count(1) 
from webtrends.evevent_hi_client_imp_all 
where dt in ('2023-11-25','2023-11-26','2023-11-27','2023-11-28','2023-11-29') 
group by dt
order by dt;


select dt,count(1)
from webtrends.evevent_hi_client_imp_all 
where dt='2023-11-27'
and hour = '00'
and event_type = 'CUSTOM_EVENT'
and area_id <> '--'
--and imp_type <> '--'
group by dt
--limit 1000
FORMAT CSV;

插码系统重点保障： 2023-12-01  17:00
整体运行正常    
一、系统巡检情况：
(1)软负载：当前连接并发量V4：22万/秒，V6：28万/秒；平均CPU、内存、磁盘使用率：18%、3%、72%。
(2)采集：collector：最高堆积35k；平均CPU、内存、磁盘使用率：60%、16%、47%
shotpot：临时文件增量正常、切割正常，平均CPU、内存、磁盘使用率：51.13%、8%、75%。
(3)入库：生产速率5.71M，消费速率4.95M，积压1.36B。
(4)CDH集群：任务调度正常，HDFS/YARN服务正常。
(5)CH集群：平均CPU、内存、磁盘使用率：46.35%、12%、66%。
二、接口巡检情况：
20231201账期：
(1)51007: 1-7点调度完成，8-15点正在运行；
(2)51006: 1-7点调度完成，8-15点正在运行；
(3)51010: 1-7点调度完成，8-15点正在运行；


select toDate(event_time) as dt,LPAD(toString(toHour(event_time)), 2, '0') as hour, count(1)
from olap.event_all
where dt = '2023-12-01'
group by dt,hour
order by dt,hour
FORMAT CSVWithNames;


1567-10877660022023-11-06 15:00:06.13854caf3be1b7406ed6e141abc64f3c0accustom_event2023-11-06 14:59:55.2894e5bb0ee-c14b-3eeb-90bd-03c83012495a13901982471$basic_userId33934339349dd825aa-6b7d-494a-afe7-e0495a744539{"WT_loginProvince":"210","WT_a_dc":"002","WT_channel":"C0021","WT_av":"APP_android_8.5.0","WT_markId":"1087766002","WT_event":"567-1087766002","WT_adverType":"567","WT_prov":"210","WT_city":"021","WT_clientID":"wgUKWvTT8sg3I3f9ACGpfCZBQPrbFIhhzfYN/PVs0F2crdr+9zRrYkz6DZkaVsugjUKj+yX4V4AeWrBs8WDzDDyAPBKrIYlcZ1vA/VPjCK5NVziSmq5n7jjsHprUpfHVrr6K1sk62AA=","WT_aav":"8.5.0","WT_userBrand":"01","WT_loginCity":"021","WT_mobile":"13901982471","WT_cid":"wgUKWvTT8sg3I3f9ACGpfCZBQPrbFIhhzfYN/PVs0F2crdr+9zRrYkz6DZkaVsugjUKj+yX4V4AeWrBs8WDzDDyAPBKrIYlcZ1vA/VPjCK5NVziSmq5n7jjsHprUpfHVrr6K1sk62AA="}
com.sh.cm.shydhnAndroid
\N
\N
\N
\N
\N
\N
\N\N\N\N\N\N\N\N\NAndroidAndroid 128.5.0C0021HUAWEIDCO-AL00PHONEPORTRAIT2616*1212zh\N9e4e5fa7244c6b6ecom.sh.cm.shydhn2409:891e:2840:1403:ec8a:9aff:fea7:188eokhttp/3.14.93.4.5\N\N\Na1f48d9ff4f42571816



select 
*
from olap.event_all
where toDate(event_time)='2023-12-07' 
--实际测试的时间
and toHour(event_time) in ('14','15','16')
and attributes['QYCS_0001'] is not null 
limit 1
FORMAT CSVWithNames;


"project_id","event_key","event_time","event_id","event_type","client_time","anonymous_user","user","user_key","gio_id","merged_gio_id","session","attributes","package","platform","referrer_domain","utm_source","utm_medium","utm_campaign","utm_term","utm_content","traffic_source","ads_id","key_word","country_code","country_name","region","city","browser","browser_version","os","os_version","client_version","channel","device_brand","device_model","device_type","device_orientation","resolution","language","referrer_type","account_id","domain","ip","user_agent","sdk_version","location_latitude","location_longitude","bot_id","data_source_id","esid","origin_data","trace_info","old_gio_id"
1,"imp","2023-12-07 16:37:35.791","18aa7ecaaccb17e94660a67bf672c780","custom_event","2023-12-07 16:38:56.917","2057ef17-936b-4633-8b3d-7d32281dcf4c","6g8iuQjkPnArsrSwr0Hx+A==","$basic_userId",2795,2795,"978cd70f-cdf1-4250-8cf5-61886eecec90","{'QYCS_0010':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item12_01"",""WT_envName"":""第十二行第一个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":2129,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0011':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item12_02"",""WT_envName"":""第十二行第二个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":19168,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','WT_mc_ev':'210315_QYCS','WT_login_status':'1','QYCS_0016':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item14_01"",""WT_envName"":""第十四行第一个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":28630,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0017':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item14_02"",""WT_envName"":""第十四行第二个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":19090,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','WT_component_id':'Rights-ThreeProductsRecommendComp','QYCS_0018':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item14_03"",""WT_envName"":""第十四行第三个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":15820,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0019':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item15_01"",""WT_envName"":""第十五行第一个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":15754,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','WT_event':'20200917_exposure','QYCS_0012':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item12_03"",""WT_envName"":""第十二行第三个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":15832,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0013':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item13_01"",""WT_envName"":""第十三行第一个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":15087,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0014':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item13_02"",""WT_envName"":""第十三行第二个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":15736,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0015':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item13_03"",""WT_envName"":""第十三行第三个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":27791,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','WT_module_name':'Rights-ThreeProductsRecommendComp_OaGiT','WT_clientID':'ki117cQKSfSnEdwm1ltieEjbl5uUMZTWppOFDwBREGbvhHgHTewk5p6LFgeAYsU4GPpXeNP372JR3eOQe3CXtt8I/qEoMAifYktbc9FfTRgmlEikX/+GH8cOJJ2/BGWEUl65v0ovfXw=','WT_userBrand':'01','WT_environment':'production','WT_cid':'ki117cQKSfSnEdwm1ltieEjbl5uUMZTWppOFDwBREGbvhHgHTewk5p6LFgeAYsU4GPpXeNP372JR3eOQe3CXtt8I/qEoMAifYktbc9FfTRgmlEikX/+GH8cOJJ2/BGWEUl65v0ovfXw=','WT_es':'https://dev.coc.10086.cn/coc3/canvas/rightsmarket-h5-canvas/online/home?channelCode=P00000060139&yx=1066658009&channelId=P00000056834&sourceId=056002','QYCS_0020':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item15_02"",""WT_envName"":""第十五行第二个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":15747,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','WT_loginProvince':'771','WT_current_url':'gxhome2023','QYCS_0021':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item15_03"",""WT_envName"":""第十五行第三个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":15918,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','$path':'/coc3/canvas/rightsmarket-h5-canvas/online/home','QYCS_0005':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item10_02"",""WT_envName"":""第十行第二个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":15936,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0006':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item10_03"",""WT_envName"":""第十行第三个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":28916,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0007':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item11_01"",""WT_envName"":""第十一行第一个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":18403,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','WT_member':'6&1','WT_current_pagename':'广西中国移动权益超市2023','QYCS_0008':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item11_02"",""WT_envName"":""第十一行第二个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":29123,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0001':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item09_01"",""WT_envName"":""第九行第一个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":14543,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0002':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item09_02"",""WT_envName"":""第九行第二个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":21364,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0003':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item09_03"",""WT_envName"":""第九行第三个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":28470,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0004':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item10_01"",""WT_envName"":""第十行第一个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":15856,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','QYCS_0009':'{""WT_et"":""imp"",""WT_event"":""gxhome2023_Rights-ThreeProductsRecommendComp_item11_03"",""WT_envName"":""第十一行第三个商品"",""WT_si_n"":""广西中国移动权益超市2023"",""WT_si_x"":""1"",""WT_sku_id"":15083,""WT_prepare1"":""P00000046400"",""WT_goods_id"":"""",""WT_serial_no"":""C00020230629002R000""}','WT_module_no':'23482','WT_pageid':'5763','$title':'中国移动权益超市','WT_channelid':'P00000056834','$query':'channelCode=P00000060139&yx=1066658009&channelId=P00000056834&sourceId=056002','WT_loginCity':'0773'}",\N,"Web",\N,\N,\N,\N,\N,\N,\N,\N,\N,"CN","中国","广西","北海","Chrome Mobile WebView","Chrome Mobile WebView 101.0.4951","Android","Android 13","1.0.0",\N,"Generic_Android","Generic_Android V2301A",\N,"PORTRAIT","801*361","zh-hans",\N,"9e4e5fa7244c6b6e","dev.coc.10086.cn","171.109.9.49","Mozilla/5.0 (Linux; Android 13; V2301A Build/TP1A.220624.014; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/101.0.4951.74 Mobile Safari/537.36 leadeon/9.3.0/CMCCIT","3.8.6",\N,\N,\N,"aba9de4ce446b2d2",43,"{""anonymousId"":""2057ef17-936b-4633-8b3d-7d32281dcf4c"",""projectKey"":""9e4e5fa7244c6b6e"",""userId"":""6g8iuQjkPnArsrSwr0Hx+A=="",""timestamp"":""1701938336917"",""esid"":43,""session"":""978cd70f-cdf1-4250-8cf5-61886eecec90"",""eventKey"":""imp"",""type"":""CUSTOM_EVENT"",""version"":""3.8.6"",""ip"":""171.109.9.49"",""context"":{""app"":{""version"":""1.0.0"",""userAgent"":""Mozilla/5.0 (Linux; Android 13; V2301A Build/TP1A.220624.014; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/101.0.4951.74 Mobile Safari/537.36 leadeon/9.3.0/CMCCIT""},""device"":{""platform"":""web""},""screen"":{""width"":361,""height"":801},""operationSystem"":{""name"":""web""},""page"":{""domain"":""dev.coc.10086.cn"",""path"":""/coc3/canvas/rightsmarket-h5-canvas/online/home"",""query"":""channelCode=P00000060139&yx=1066658009&channelId=P00000056834&sourceId=056002"",""title"":""中国移动权益超市""},""referrer"":{},""location"":{},""locale"":{""language"":""zh-CN""}},""attributes"":{""WT_cid"":""ki117cQKSfSnEdwm1ltieEjbl5uUMZTWppOFDwBREGbvhHgHTewk5p6LFgeAYsU4GPpXeNP372JR3eOQe3CXtt8I/qEoMAifYktbc9FfTRgmlEikX/+GH8cOJJ2/BGWEUl65v0ovfXw="",""WT_clientID"":""ki117cQKSfSnEdwm1ltieEjbl5uUMZTWppOFDwBREGbvhHgHTewk5p6LFgeAYsU4GPpXeNP372JR3eOQe3CXtt8I/qEoMAifYktbc9FfTRgmlEikX/+GH8cOJJ2/BGWEUl65v0ovfXw="",""WT_userBrand"":""01"",""WT_loginProvince"":""771"",""WT_loginCity"":""0773"",""WT_es"":""https://dev.coc.10086.cn/coc3/canvas/rightsmarket-h5-canvas/online/home?channelCode=P00000060139&yx=1066658009&channelId=P00000056834&sourceId=056002"",""WT_channelid"":""P00000056834"",""WT_last_pageid"":"""",""WT_last_url"":"""",""WT_last_pagename"":"""",""WT_pageid"":""5763"",""WT_module_no"":""23482"",""WT_module_name"":""Rights-ThreeProductsRecommendComp_OaGiT"",""WT_current_url"":""gxhome2023"",""WT_current_pagename"":""广西中国移动权益超市2023"",""WT_member"":""6&1"",""WT_goods_no"":"""",""WT_login_status"":""1"",""WT_event"":""20200917_exposure"",""WT_next_url"":"""",""WT_next_pageid"":"""",""WT_next_pagename"":"""",""WT_component_id"":""Rights-ThreeProductsRecommendComp"",""WT_environment"":""production"",""WT_mc_ev"":""210315_QYCS"",""QYCS_0001"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item09_01\"",\""WT_envName\"":\""第九行第一个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":14543,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0002"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item09_02\"",\""WT_envName\"":\""第九行第二个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":21364,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0003"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item09_03\"",\""WT_envName\"":\""第九行第三个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":28470,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0004"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item10_01\"",\""WT_envName\"":\""第十行第一个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":15856,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0005"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item10_02\"",\""WT_envName\"":\""第十行第二个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":15936,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0006"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item10_03\"",\""WT_envName\"":\""第十行第三个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":28916,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0007"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item11_01\"",\""WT_envName\"":\""第十一行第一个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":18403,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0008"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item11_02\"",\""WT_envName\"":\""第十一行第二个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":29123,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0009"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item11_03\"",\""WT_envName\"":\""第十一行第三个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":15083,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0010"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item12_01\"",\""WT_envName\"":\""第十二行第一个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":2129,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0011"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item12_02\"",\""WT_envName\"":\""第十二行第二个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":19168,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0012"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item12_03\"",\""WT_envName\"":\""第十二行第三个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":15832,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0013"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item13_01\"",\""WT_envName\"":\""第十三行第一个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":15087,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0014"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item13_02\"",\""WT_envName\"":\""第十三行第二个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":15736,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0015"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item13_03\"",\""WT_envName\"":\""第十三行第三个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":27791,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0016"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item14_01\"",\""WT_envName\"":\""第十四行第一个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":28630,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0017"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item14_02\"",\""WT_envName\"":\""第十四行第二个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":19090,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0018"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item14_03\"",\""WT_envName\"":\""第十四行第三个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":15820,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0019"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item15_01\"",\""WT_envName\"":\""第十五行第一个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":15754,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0020"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item15_02\"",\""WT_envName\"":\""第十五行第二个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":15747,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}"",""QYCS_0021"":""{\""WT_et\"":\""imp\"",\""WT_event\"":\""gxhome2023_Rights-ThreeProductsRecommendComp_item15_03\"",\""WT_envName\"":\""第十五行第三个商品\"",\""WT_si_n\"":\""广西中国移动权益超市2023\"",\""WT_si_x\"":\""1\"",\""WT_sku_id\"":15918,\""WT_prepare1\"":\""P00000046400\"",\""WT_goods_id\"":\""\"",\""WT_serial_no\"":\""C00020230629002R000\""}""},""sendTime"":""1701938255791"",""dataSourceId"":""aba9de4ce446b2d2"",""item"":{},""userKey"":""$basic_userId""}","{}",\N