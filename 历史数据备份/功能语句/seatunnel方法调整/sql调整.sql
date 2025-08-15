create function extract_xy_no_lag_lat as 'com.udf2.ExtractJsonUDF2' using jar 'hdfs://udbachdp1/hive-udf/udf-ExtractUDF-XY-1.2.1.jar';

userAgentParser

create function userAgentParser_test5 as 'com.udf.UserAgentParser' using jar 'hdfs://udbachdp1/hive-udf/useragent.1.0.2.jar';

attributes['System'] 

select user_agent,userAgentParser_test5(user_agent) from ham_jituan.seatunnel_cdpevent where dt='2024-01-05' and hour='01' limit 10;


Mozilla/5.0 (Linux; U; Android 14; zh-CN; V2243A Build/UP1A.231005.007) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/69.0.3497.100 UWS/3.22.2.65 Mobile Safari/537.36 UCBS/3.22.2.65_230627202730 AlipayDefined AriverApp(mPaaSClient/10.2.8) MiniProgram  leadeon/9.4.1/CMCCIT/tinyApplet,{"os":"Android","os_version":"Android 14"}
Mozilla/5.0 (Linux; Android 13; M2104K10AC Build/TP1A.220624.014; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/104.0.5112.97 Mobile Safari/537.36 leadeon/9.5.0/CMCCIT,{"os":"Android","device_model":"Redmi Note 10 Pro","os_version":"Android 13"}
pEDMw4SNwkDMyIjLBFDUU9CZslWdCBCMx00RHBFI7MTMgQWavJHZuFEI7UFI7gXdulGToACMuEjLy8yapZHbhR0OpQWavJHZuF0XwBXQosSMxMjMzAjMyAjMi9FMuAjL08SeyFmcilGT05WZpx2QzRmblJHdiV2V,{"os":"Other","os_version":"Other null"}
Mozilla/5.0 (Linux; Android 12; ALA-AN70 Build/HONORALA-AN70; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.72 MQQBrowser/6.2 TBS/046247 Mobile Safari/537.36 leadeon/7.8.0/CMCCIT,{"os":"Android","os_version":"Android 12"}
pYTMw4iMxgDMxIjLBFDUT9CZslWdCBCMxEDSKBFI7QTMgQWavJHZuFEI7UFI7gXdulGToACMuEjLy8yapZHbhR0OpQWavJHZuF0XwBXQosSMxMjMzAjMyAjMi9FMuAjL08SeyFmcilGT05WZpx2QzRmblJHdiV2V,{"os":"Other","os_version":"Other null"}
Mozilla/5.0 (iPhone; CPU iPhone OS 16_1_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148/wkwebview leadeon/9.5.0/CMCCIT,{"os":"iOS","os_version":"iOS 16"}
QKxEDMuAjM3ADMy4SQxAlUvQGbpVnQgMUQ2ETMxkDMxIDI7ETMgQWavJHZuFEI7UFI7gXdulGToACMuEjLy8yapZHbhR0OpQWavJHZuF0XwBXQosSMxMjMzAjMyAjMi9FMuAjL08SeyFmcilGT05WZpx2QzRmblJHdiV2V,{"os":"Other","os_version":"Other null"}
Mozilla/5.0 (iPhone; CPU iPhone OS 15_8 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/19H370 Ariver/1.0.15 leadeon/9.5.0/CMCCIT/tinyApplet WK RVKType(0) NebulaX/1.0.0,{"os":"iOS","os_version":"iOS 15"}
Mozilla/5.0 (Linux; Android 11; FIO-BD00 Build/PTACFIO-BD00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/92.0.4515.105 Mobile Safari/537.36_channel=sdmccApp,{"os":"Android","os_version":"Android 11"}
Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148/wkwebview leadeon/9.5.0/CMCCIT,{"os":"iOS","os_version":"iOS 17"}