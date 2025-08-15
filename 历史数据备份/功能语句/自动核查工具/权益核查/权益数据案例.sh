第一个节点:监控clickhouse是否入库完成


com.gio.app.App51007GIOExposureStream

kafka-console-consumer.sh --bootstrap-server centos1:9092 --topic test-input

kafka-console-consumer.sh --bootstrap-server centos1:9092 --topic test-input
kafka-console-consumer.sh --bootstrap-server centos1:9092 --topic test-output --from-beginning

kafka-console-producer.sh --broker-list   centos1:9092 --topic test-input



{"attributes":{"$data_source_id":"aba9de4ce446b2d2","userId":"17637522341","userKey":"$basic_userId"}



{"accountId":"9e4e5fa7244c6b6e","anonymousUser":"a1ab5ff2-3249-4015-85e0-6e530564810d","clientTime":1702518902504,"esId":44,"eventKey":"imp","eventTime":1702518855595,"eventType":"CUSTOM_EVENT","locationLatitude":0.0,"locationLongitude":0.0,"packageName":"","session":"6c6e9ea5-6359-4275-8cae-e72fdbccd4ea","userId":"17637522341","userKey":"$basic_userId"}

17637525296

{"accountId":"9e4e5fa7244c6b6e","anonymousUser":"a1ab5ff2-3249-4015-85e0-6e530564810d","clientTime":1702518902504,"esId":44,"eventKey":"imp","eventTime":1702518855595,"eventType":"CUSTOM_EVENT","locationLatitude":0.0,"locationLongitude":0.0,"packageName":"","session":"6c6e9ea5-6359-4275-8cae-e72fdbccd4ea","userId":"17637525296","userKey":"$basic_userId"}

/usr/local/postgresql/bin/pg_ctl -D /usr/local/postgresql/pgsqldata/ -l /usr/local/postgresql/pgsqldata/logs/pgsql.log start



{"accountId":"9e4e5fa7244c6b6e","anonymousUser":"a1ab5ff2-3249-4015-85e0-6e530564810d","attributes":{"$data_source_id":"aba9de4ce446b2d2","WT_goods_id":"","WT_mc_ev":"210315_QYCS","WT_login_status":"1","$index":"0","$os":"web","WT_component_id":"Rights-SwiperComp","$platform":"web","$ip":"2409:890e:ee48:10ab:69ab:4885:b37d:abb2","WT_event":"sxhome2023_Rights-SwiperComp_banner01","$device_orientation":"PORTRAIT","$user_agent":"Mozilla/5.0 (Linux; Android 12; ELS-AN00 Build/HUAWEIELS-AN00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/111.0.5563.116 Mobile Safari/537.36 XWEB/1110039 MMWEBSDK/20231002 MMWEBID/2229 MicroMessenger/8.0.43.2480(0x28002B51) WeChat/arm64 Weixin NetType/5G Language/zh_CN ABI/arm64","$language":"zh-CN","WT_envName":"banner第一帧_文旅出行","WT_module_name":"Rights-SwiperComp_k0USr","WT_last_url":"","WT_environment":"production","$client_version":"1.0.0","WT_prepare1":"","WT_et":"imp","WT_es":"https://dev.coc.10086.cn/coc3/canvas/rightsmarket-h5-canvas/online/home?channelCode=P00000005556&token=YZsidmicrof2f87871bdaf40b0adcba872963d3ddb","WT_current_url":"sxhome2023","WT_si_n":"山西中国移动权益超市2023","WT_last_pageid":"","WT_last_pagename":"","$path":"/coc3/canvas/rightsmarket-h5-canvas/online/home","WT_next_url":"","WT_goods_no":"","$sdk_version":"3.8.6","WT_member":"2&5","WT_current_pagename":"山西中国移动权益超市2023","WT_next_pagename":"","WT_next_pageid":"","WT_sku_id":"","WT_si_x":"1","WT_module_no":"46689","WT_pageid":"5767","WT_serial_no":"","$title":"中国移动权益超市","WT_channelid":"P00000005556","$referrer_domain":"https://dev.coc.10086.cn/coc3/canvas/rightsmarket-h5-canvas/online/home?channelCode=P00000005556","$query":"channelCode=P00000005556&token=YZsidmicrof2f87871bdaf40b0adcba872963d3ddb","$domain":"dev.coc.10086.cn"},"clientTime":1702518902504,"esId":44,"eventKey":"imp","eventTime":1702518855595,"eventType":"CUSTOM_EVENT","locationLatitude":0.0,"locationLongitude":0.0,"packageName":"","session":"6c6e9ea5-6359-4275-8cae-e72fdbccd4ea","userId":"13948869613","userKey":"$basic_userId"}

{"accountId":"9e4e5fa7244c6b6e","anonymousUser":"a1ab5ff2-3249-4015-85e0-6e530564810d","attributes":{"$data_source_id":"aba9de4ce446b2d2","WT_goods_id":"","WT_mc_ev":"210315_QYCS","WT_login_status":"1","$index":"0","$os":"web","WT_component_id":"Rights-SwiperComp","$platform":"web","$ip":"2409:890e:ee48:10ab:69ab:4885:b37d:abb2","WT_event":"sxhome2023_Rights-SwiperComp_banner01","$device_orientation":"PORTRAIT","$user_agent":"Mozilla/5.0 (Linux; Android 12; ELS-AN00 Build/HUAWEIELS-AN00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/111.0.5563.116 Mobile Safari/537.36 XWEB/1110039 MMWEBSDK/20231002 MMWEBID/2229 MicroMessenger/8.0.43.2480(0x28002B51) WeChat/arm64 Weixin NetType/5G Language/zh_CN ABI/arm64","$language":"zh-CN","WT_envName":"banner第一帧_文旅出行","WT_module_name":"Rights-SwiperComp_k0USr","WT_last_url":"","WT_environment":"production","$client_version":"1.0.0","WT_prepare1":"","WT_et":"imp","WT_es":"https://dev.coc.10086.cn/coc3/canvas/rightsmarket-h5-canvas/online/home?channelCode=P00000005556&token=YZsidmicrof2f87871bdaf40b0adcba872963d3ddb","WT_current_url":"sxhome2023","WT_si_n":"山西中国移动权益超市2023","WT_last_pageid":"","WT_last_pagename":"","$path":"/coc3/canvas/rightsmarket-h5-canvas/online/home","WT_next_url":"","WT_goods_no":"","$sdk_version":"3.8.6","WT_member":"2&5","WT_current_pagename":"山西中国移动权益超市2023","WT_next_pagename":"","WT_next_pageid":"","WT_sku_id":"","WT_si_x":"1","WT_module_no":"46689","WT_pageid":"5767","WT_serial_no":"","$title":"中国移动权益超市","WT_channelid":"P00000005556","$referrer_domain":"https://dev.coc.10086.cn/coc3/canvas/rightsmarket-h5-canvas/online/home?channelCode=P00000005556","$query":"channelCode=P00000005556&token=YZsidmicrof2f87871bdaf40b0adcba872963d3ddb","$domain":"dev.coc.10086.cn"},"clientTime":1702518902504,"esId":44,"eventKey":"imp","eventTime":1702518855595,"eventType":"CUSTOM_EVENT","locationLatitude":0.0,"locationLongitude":0.0,"packageName":"","session":"6c6e9ea5-6359-4275-8cae-e72fdbccd4ea","userId":"3333","userKey":"$basic_userId"}

CREATE TABLE data_white1 (WT_COOKIES varchar(100) COLLATE "pg_catalog"."default", BILL_NO varchar(100) COLLATE "pg_catalog"."default", DATA_TIME varchar(100) COLLATE "pg_catalog"."default", WT_EVENT_ID varchar(100) COLLATE "pg_catalog"."default", WT_ET varchar(20) COLLATE "pg_catalog"."default", WT_CURRENT_URL varchar(100) COLLATE "pg_catalog"."default", WT_COMPONENT_ID varchar(100) COLLATE "pg_catalog"."default", WT_MODULE_NO varchar(100) COLLATE "pg_catalog"."default", WT_POINT_POSITION varchar(100) COLLATE "pg_catalog"."default", WT_ENVNAME varchar(100) COLLATE "pg_catalog"."default", WT_PREPARE1 varchar(100) COLLATE "pg_catalog"."default");



{"attributes":{"$data_source_id":"aba9de4ce446b2d2","WT_prepare1":"aaaa"},"userId":"13948869613","userKey":"$basic_userId"}

{"attributes":{"$data_source_id":"aba9de4ce446b2d2","WT_prepare1":"13948869613"},"userId":"222","userKey":"$basic_userId"}

a1f48d9ff4f42571
b508a809cbbddd0b


{"attributes":{"$data_source_id":"a1f48d9ff4f42571","WT_prepare1":"aaaa","WT_es":"https://dev.coc.10086.cn/coc3/canvas/rightsmarket-h5-canvas","WT_mc_ev":"210315_QYCS"},"userId":"13948869613","userKey":"$basic_userId","eventType":"CUSTOM_EVENT"}

{"attributes":{"$data_source_id":"a1f48d9ff4f42571","WT_prepare1":"aaaa","WT_es":"httsps://dev.coc.10086.cn/coc3/canvas/rightsmarket-h5-canvas","WT_mc_ev":"210315_QYCSww"},"userId":"13948869613","userKey":"$basic_userId","eventType":"CUSTOM_EVENT"}