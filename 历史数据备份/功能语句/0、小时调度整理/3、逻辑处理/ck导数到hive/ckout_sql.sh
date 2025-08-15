#!/bin/sh
ip=$1
dt=$2
hour=$3


clickhouse-client -h "$ip" --format_csv_delimiter=$'\001'  --query="
select project_id,event_key,event_time,event_id,event_type,client_time,anonymous_user,user,user_key,gio_id,merged_gio_id,session,attributes,package,platform,referrer_domain,utm_source,utm_medium,utm_campaign,utm_term,utm_content,traffic_source,ads_id,key_word,country_code,country_name,region,city,browser,browser_version,os,os_version,client_version,channel,device_brand,device_model,device_type,device_orientation,resolution,language,referrer_type,account_id,domain,ip,user_agent,sdk_version,location_latitude,location_longitude,bot_id,data_source_id,esid from olap.event_local
where toYYYYMMDD(event_time)='$dt'
and (case when toHour(event_time) < 10 then concat('0',toString(toHour(event_time))) else toString(toHour(event_time)) end)='$hour'
and data_source_id in ('b95440ef47ec01fc','a1f48d9ff4f42571','b508a809cbbddd0b','90be4403373b6463') 
and event_type='custom_event'
FORMAT CSV" | sed 's/"//g' | sed "s/'/\"/g" > /data/datahql/data_ck_"$dt"_"$hour"_"$ip".csv