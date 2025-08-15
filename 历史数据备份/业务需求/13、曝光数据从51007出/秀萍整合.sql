SELECT 
T1.area_id   
,T1.wt_event  
,T1.pv       AS  BG_pv 
,T1.uv       AS  BG_uv  
,T1.user     AS  BG_user
,T2.pv       AS  DG_pv
,T2.pv/T1.pv AS  pv_RATION   
,T2.uv       AS  DG_uv
,T2.uv/T1.uv AS  uv_RATION  
,T2.user  AS  DG_user
,T2.user/T1.user  AS  user_RATION
FROM 
(SELECT
    area_id,
    wt_event,
    count(1) AS pv,
    countDistinct(wt_co_f) AS uv,
    countDistinct(wt_mobile) AS user
FROM webtrends.event_hi_client_all
WHERE (et = 'imp') AND (imp_type = 'once') AND (dt = '2023-05-30')
GROUP BY
    area_id,
    wt_event
)T1
LEFT JOIN 
(
SELECT
    wt_event,
    count(1) AS pv,
    countDistinct(wt_co_f) AS uv,
    countDistinct(wt_mobile) AS user
FROM webtrends.event_hi_client_all
WHERE (et = 'clk') AND (dt = '2023-05-30')
GROUP BY wt_event
)T2
ON T1.wt_event = T2.wt_event
FORMAT CSVWithNames;




[{"eventType":"CUSTOM","eventName":"imp","pageShowTimestamp":1689315302209,"attributes":{"WT_cid":"ExU/kxw7zxt2SVqUHIkVKyQBwGV1Jq6LS0HzrF8462dg2bgZNjxyFlYzi+cK5Nqs1lKiVMlQ7N/xZq77zAVkP5SfMOUfbJ6tkF8Ybre11YcHRbVYprat1aG1mowCsQREAE9fzMplSkw=","WT_clientID":"ExU/kxw7zxt2SVqUHIkVKyQBwGV1Jq6LS0HzrF8462dg2bgZNjxyFlYzi+cK5Nqs1lKiVMlQ7N/xZq77zAVkP5SfMOUfbJ6tkF8Ybre11YcHRbVYprat1aG1mowCsQREAE9fzMplSkw=","WT_prov":"250","WT_city":"0513","WT_userBrand":"03","WT_loginProvince":"250","WT_loginCity":"0513","userId":"17260258360","area_id":"20221213002","WT_et":"imp","type":"once","P00000025521":"{\"WT_event\":\"P00000025521\",\"WT_envname\":\"首页plus版_仪表盘区域_通话\",\"WT_next_url\":\"https://h.app.coc.10086.cn/cmcc-app/setMealSurplus/setMealSurplus.html?formHomePlustabIndex=1\"}","P00000025522":"{\"WT_event\":\"P00000025522\",\"WT_envname\":\"首页plus版_仪表盘区域_通用流量\",\"WT_next_url\":\"https://h.app.coc.10086.cn/cmcc-app/setMealSurplus/setMealSurplus.html\"}","P00000025523":"{\"WT_event\":\"P00000025523\",\"WT_envname\":\"首页plus版_仪表盘区域_其他流量\",\"WT_next_url\":\"https://h.app.coc.10086.cn/cmcc-app/setMealSurplus/setMealSurplus.html\"}","P00000025131":"{\"WT_event\":\"P00000025131\",\"WT_envname\":\"签到领奖\",\"WT_markId\":\"2023052615165980336154\",\"WT_next_url\":\"https://wap.js.10086.cn/nact/resource/2572/html/index.html?channelId=P00000025131&yx=2023052615165980336154\"}","P00000025132":"{\"WT_event\":\"P00000025132\",\"WT_envname\":\"E豆小店\",\"WT_markId\":\"2023071216281410354990\",\"WT_next_url\":\"https://wap.js.10086.cn/nact/resource/2504/html/index.html?channelId=P00000025132&yx=2023071216281410354990\"}","P00000025133":"{\"WT_event\":\"P00000025133\",\"WT_envname\":\"查账单\",\"WT_markId\":\"2023052615165982736155\",\"WT_next_url\":\"https://h.app.coc.10086.cn/cmcc-app/personalBill/phoneBillsNew.html?channelId=P00000025133&yx=2023052615165982736155\"}","P00000025134":"{\"WT_event\":\"P00000025134\",\"WT_envname\":\"钱包\",\"WT_markId\":\"2023071216281419554992\",\"WT_next_url\":\"https://fintech.12580life.com/fintech-h5/fortuneCenter/walletCardcenter?channelId=P00000025134&yx=2023071216281419554992\"}","P00000025135":"{\"WT_event\":\"P00000025135\",\"WT_envname\":\"玩积分\",\"WT_markId\":\"2023071216281421854993\",\"WT_next_url\":\"https://wap.js.10086.cn/vw/JFDHINDEXNEW?ch=7x&channelId=P00000025135&yx=2023071216281421854993\"}","P00000025136":"{\"WT_event\":\"P00000025136\",\"WT_envname\":\"我的家\",\"WT_markId\":\"2023071216281429254994\",\"WT_next_url\":\"https://wap.js.10086.cn/vw/WDJT2021?ch=7x&channelId=P00000025136&yx=2023071216281429254994\"}","P00000025524":"{\"WT_event\":\"P00000025524\",\"WT_envname\":\"消费\",\"WT_next_url\":\"https://h.app.coc.10086.cn/cmcc-app/personalBill/phoneBillsNew.html\"}","P00000025525":"{\"WT_event\":\"P00000025525\",\"WT_envname\":\"余额\",\"WT_next_url\":\"https://h.app.coc.10086.cn/leadeon-cmcc-static/v2.0/pages/recharge/recharge.html\"}","P00000025526":"{\"WT_event\":\"P00000025526\",\"WT_envname\":\"积分\",\"WT_next_url\":\"http://app.jf.10086.cn\"}"},"appVersion":"1.0.0","dataSourceId":"b95440ef47ec01fc","deviceId":"cf5a343d-45d5-4145-8f74-8c1e8493cdfc","domain":"h.app.coc.10086.cn","gioId":"17260258360","language":"zh-CN","path":"/cmcc-app/homePlusNew/index.html","platform":"web","screenHeight":823,"screenWidth":381,"sdkVersion":"3.8.3","sessionId":"17971a43-88ba-4f54-944f-06613785f659","timestamp":1689315303001,"title":"中国移动手机营业厅首页PLUS2.0","userId":"17260258360","globalSequenceId":20,"eventSequenceId":16}]