select 
wt_event,
advertype,
mark_id,
attributes 
from evevent_hi_client_imp_all 
where dt = '2023-12-15'
limit 1
FORMAT CSVWithNames;

 select wt_event,count(1) from evevent_hi_client_imp_all where dt = '2023-12-24' group by wt_event;


 {"accountId":"9e4e5fa7244c6b6e","anonymousUser":"9fed1380-b0f8-4ad7-bd68-cda25d5ea463","attributes":{"$data_source_id":"b95440ef47ec01fc","P00000054931":"{\"WT_es\":\"https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html\",\"WT_ti\":\"中国移动手机营业厅首页\",\"WT_et\":\"imp\",\"WT_envName\":\"老客有福利\",\"WT_event\":\"P00000054931\",\"WT_next_url\":\"https://wap.jx.10086.cn/hui/release/act/hffb.html?utm_source=YJchuchuang&channelId=P00000054931&yx=1084663001\",\"WT_markId\":\"1084663001\",\"WT_serial_no\":\"\"}","$index":"0","$os":"web","P00000054930":"{\"WT_es\"\"https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html\",\"WT_ti\":\"中国移动手机营业厅首页\",\"WT_et\":\"imp\",\"WT_envName\":\"消消乐-橱窗3\",\"WT_event\":\"P00000054930\",\"WT_next_url\":\"https://wx.10086.cn/qwhdhub/newbuildgroup/1023092715?A_C_CODE=clUitveLCw&channelId=P00000054930&yx=1098348001\",\"WT_markId\":\"1098348001\",\"WT_serial_no\":\"\"}","$platform":"web","$ip":"218.65.5.146","area_id":"20230710058_23","type":"once","$device_orientation":"PORTRAIT","$user_agent":"Mozilla/5.0 (Linux; Android 13; V2231A Build/TP1A.220624.014; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/101.0.4951.74 Mobile Safari/537.36 leadeon/9.4.1/CMCCIT","$language":"zh-CN","WT_prov":"791","WT_clientID":"B+Pm69bOa6v4DpkxOeBDdwPrrTmpISSSEHopkkiLDfTpPw9hibMwf9EfgEMGqoyZoUYXN3nJFAmSJPZiCPrX9dqlpfdy1tfOVykyZgF7m/fDozgaNu+xwGfDBsA5+Yj2IrsWX/nynak=","WT_userBrand":"09","$client_version":"1.0.0","WT_cid":"B+Pm69bOa6v4DpkxOeBDdwPrrTmpISSSEHopkkiLDfTpPw9hibMwf9EfgEMGqoyZoUYXN3nJFAmSJPZiCPrX9dqlpfdy1tfOVykyZgF7m/fDozgaNu+xwGfDBsA5+Yj2IrsWX/nynak=","WT_loginProvince":"791","WT_av":"APP_android_9.4.1","$path":"/cmcc-app/app-pages/home.html","$sdk_version":"3.8.6-rc.2","WT_city":"0792","WT_aav":"9.4.1","$title":"中国移动手机营业厅首页","WT_loginCity":"0792","$domain":"h.app.coc.10086.cn","P00000054928":":\"https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html\",\"WT_ti\":\"中国移动手机营业厅首页\",\"WT_et\":\"imp\",\"WT_envName\":\"网龄回馈\",\"WT_event\":\"P00000054928\",\"WT_next_url\"\"https://wap.jx.10086.cn/hui/release/activity/netage/index.html?utm_source=YJchuchuang&channelId=P00000054928&yx=1084681003\",\"WT_markId\":\"1084681003\",\"WT_serial_no\":\"\"}","P00000054929":"{\"WT_es\":\"https://h.app.coc.10086.cn/cmcc-app/app-pages/home.html\",\"WT_ti\":\"中国移动手机营业厅首页\",\"WT_et\":\"imp\",\"WT_envName\":\"转转贝\",\"WT_event\":\"P00000054929\",\"WT_next_url\":\"https://wap.jx.10086.cn/hui/shua/index?utm_source=YJchuchuang&channelId=P00000054929&yx=1084672001\",\"WT_markId\":\"1084672001\",\"WT_serial_no\":\"\"}"},"clientTime":1703575147440,"esId":9,"eventKey":"imp","eventTime":1703575148085,"eventType":"CUSTOM_EVENT","locationLatitude":0.0,"locationLongitude":0.0,"packageName":"","session":"148efaca-643d-4f27-adc4-46da680d19aa","userId":"18779227737","userKey":"$basic_userId"}


select mark_id,count(*) from evevent_hi_client_imp_all where dt = '2024-01-12' group by mark_id;

select wt_event,count(*) from evevent_hi_client_imp_all where dt = '2024-01-09' group by wt_event;
select wt_event,count(*) from evevent_hi_client_imp_all group by wt_event;

select wt_event,count(*) from evevent_hi_client_imp_all where dt = '2024-01-12' group by wt_event;



select dt,hour,count(*) from evevent_hi_client_imp_all where dt = '2024-01-09' and attributes is null group by dt,hour;

select count(*) from evevent_hi_client_imp_all where dt = '2024-01-10' and attributes = '--';

"insert into evevent_hi_client_imp_all(aa,bb) values(?,? ) " +
            "values(?,?,?,?,?,?,?,?,?,?,?