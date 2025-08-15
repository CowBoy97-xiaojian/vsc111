#!/bin/bash
ip=$1
dt=$2
hour=$3

remot_temp_file=/data1/hour_output/data_ck_"$dt"_"$hour"_"$ip".csv

airflow_file=/home/udbac/output/data_h5/data_ck_"$dt"_"$hour"_"$ip".csv

# 使用 SSH 连接到远程服务器并查找进程
ssh_output=$(ssh app@"$ip" "ps -ef | grep -v grep | grep $remot_temp_file")

# 检查是否找到了进程
if [ -n "$ssh_output" ]; then
    # 提取进程的 PID
    pid=$(echo "$ssh_output" | awk '{print $2}')

    # 终止进程
    ssh app@"$ip" "kill $pid"
fi

# 执行远程命令
# exp_result=$(
ssh -q app@"$ip"  "clickhouse-client -h '$ip' --format_csv_delimiter=$'\001'  --query=\"
select project_id,event_key,event_time,event_id,event_type,client_time,anonymous_user,user,user_key,gio_id,merged_gio_id,session,attributes,package,platform,referrer_domain,utm_source,utm_medium,utm_campaign,utm_term,utm_content,traffic_source,ads_id,key_word,country_code,country_name,region,city,browser,browser_version,os,os_version,client_version,channel,device_brand,device_model,device_type,device_orientation,resolution,language,referrer_type,account_id,domain,ip,user_agent,sdk_version,location_latitude,location_longitude,bot_id,data_source_id,esid from olap.event_local
where toYYYYMMDD(event_time)='$dt'
and (case when toHour(event_time) < 10 then concat('0',toString(toHour(event_time))) else toString(toHour(event_time)) end)='$hour'
and data_source_id in ('8a568ccb742bdc43','9249b85f328e4e9e','9a58d221e7eadf87','8177b467fba9fc0b','aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2','a797613d647af6c6','a7464be8b200fe24','b9e74b9d5da046ce','b87eb8cec01051a6','b098587a3af15a25','b23efeab1461679b','a10b1f67c7d288c1','ae2308d5a08258b6','a04f0ad9925c080f','a6ba03d6884ee1c1','aa3a7b170aa41eed','a441e7427a44d390','adae9b8274a853f1','ad2e89b1010aed1e','bfe72e945deea64f','81ebc780373ee7cd','8d5d4407d1c3f6bf','abd580f08cf381e0','ba81a7647a8bed5d','8c19b47f237e28d3','a609de73d1690c0e','9c294cf845907ac1','b0839305cf5d280d','9a238b92b6612604','afdbd0f6b9a7ba30','8273883304ad70fb','86fdf7160c5cc8cc','81fab8d01c17ee89','89e7161d4a6b817f','9b50668a016469ec','bda85eeb52fb8b87','a8c29f7abfd639b4','b8d9fd437de471e4','875444ff6e049f33','853d176d98834573','ac81a55cdf7074d5','aae6afbf8823b1c1','999264cd2e773538','beb5471c243b2971','b5b9ccb1f69a35b9','8e76b40944f88ee5','9f8770a1dbd09047') 
and event_type='custom_event'
FORMAT CSV\" | sed 's/\"//g' | sed \"s/'/\\\"/g\" > $remot_temp_file" #)

if [ $? -eq 0 ]; then
    echo "$ip 成功导出本地文件，开始转发到181"
    #scp_result=$(
    scp app@"$ip":"$remot_temp_file" "$airflow_file"
    #)
    if [ $? -eq 0 ]; then
        echo "$ip 成功转发文件到181"
        ssh -q app@"$ip" "rm -rf $remot_temp_file"
    fi
    
fi



[udbac@new-airflow-2 ~/bin]$ cat ckout_51006_sql.sh
#!/bin/sh
ip=$1
dt=$2
hour=$3


clickhouse-client -h "$ip" --format_csv_delimiter=$'\001'  --query="
select project_id,event_key,event_time,event_id,event_type,client_time,anonymous_user,user,user_key,gio_id,merged_gio_id,session,attributes,package,platform,referrer_domain,utm_source,utm_medium,utm_campaign,utm_term,utm_content,traffic_source,ads_id,key_word,country_code,country_name,region,city,browser,browser_version,os,os_version,client_version,channel,device_brand,device_model,device_type,device_orientation,resolution,language,referrer_type,account_id,domain,ip,user_agent,sdk_version,location_latitude,location_longitude,bot_id,data_source_id,esid from olap.event_local
where toYYYYMMDD(event_time)='$dt'
and (case when toHour(event_time) < 10 then concat('0',toString(toHour(event_time))) else toString(toHour(event_time)) end)='$hour'
and data_source_id in ('8a568ccb742bdc43','9249b85f328e4e9e','9a58d221e7eadf87','8177b467fba9fc0b','aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2','a797613d647af6c6','a7464be8b200fe24','b9e74b9d5da046ce','b87eb8cec01051a6','b098587a3af15a25','b23efeab1461679b','a10b1f67c7d288c1','ae2308d5a08258b6','a04f0ad9925c080f','a6ba03d6884ee1c1','aa3a7b170aa41eed','a441e7427a44d390','adae9b8274a853f1','ad2e89b1010aed1e','bfe72e945deea64f','81ebc780373ee7cd','8d5d4407d1c3f6bf','abd580f08cf381e0','ba81a7647a8bed5d','8c19b47f237e28d3','a609de73d1690c0e','9c294cf845907ac1','b0839305cf5d280d','9a238b92b6612604','afdbd0f6b9a7ba30','8273883304ad70fb','86fdf7160c5cc8cc','81fab8d01c17ee89','89e7161d4a6b817f','9b50668a016469ec','bda85eeb52fb8b87','a8c29f7abfd639b4','b8d9fd437de471e4','875444ff6e049f33','853d176d98834573','ac81a55cdf7074d5','aae6afbf8823b1c1','999264cd2e773538','beb5471c243b2971','b5b9ccb1f69a35b9','8e76b40944f88ee5','9f8770a1dbd09047') 
and event_type='custom_event'
FORMAT CSV" | sed 's/"//g' | sed "s/'/\"/g" > /home/udbac/output/data_h5/data_ck_"$dt"_"$hour"_"$ip".csv

[udbac@new-airflow-2 ~/bin]$ cat ckout_51007_sql.sh
#!/bin/sh
ip=$1
dt=$2
hour=$3


clickhouse-client -h "$ip" --format_csv_delimiter=$'\001'  --query="
select project_id,event_key,event_time,event_id,event_type,client_time,anonymous_user,user,user_key,gio_id,merged_gio_id,session,attributes,package,platform,referrer_domain,utm_source,utm_medium,utm_campaign,utm_term,utm_content,traffic_source,ads_id,key_word,country_code,country_name,region,city,browser,browser_version,os,os_version,client_version,channel,device_brand,device_model,device_type,device_orientation,resolution,language,referrer_type,account_id,domain,ip,user_agent,sdk_version,location_latitude,location_longitude,bot_id,data_source_id,esid from olap.event_local
where toYYYYMMDD(event_time)='$dt'
and (case when toHour(event_time) < 10 then concat('0',toString(toHour(event_time))) else toString(toHour(event_time)) end)='$hour'
and data_source_id in ('ac36424553dcd3e8','b95440ef47ec01fc','a1f48d9ff4f42571','b508a809cbbddd0b','90be4403373b6463','86596eaccd0d746a','8aeb9b26885f3d8b','bdf908bd8e07b82c','938892fec03694af','b00057b79cbf85af','92f9b8b42859ed1c','94bbabc3a9686c5a','ad45b0b4c1ef7446','a9ae56608c62f805','ad3f51110dccb587','ad7c40e8ac8a0983','826f8be0db16938b','9328255238347f80','a930f2f2aee66a7c','b1b4618c1d4fac12','bee33e74dc5e9b38','aa4dbfdc0e193192','af82bebd8421abec','be5412a41f02e47a','8dd990e550265ae5','9c51cb5ab2e5d077','a20bffba73210972','9ead359aaf617556','98e2f7b831f876dd','aebed7d26ca2d38a','ab6b0c4315fa502b','9ed39aa37260081e','806586170173099d','ac34c865ecb163fa','a47b49395334c862','9e488aa27d948855','ae1f482357600a77') 
and event_type='custom_event'
FORMAT CSV" | sed 's/"//g' | sed "s/'/\"/g" > /data/datahql/data_ck_"$dt"_"$hour"_"$ip".csv