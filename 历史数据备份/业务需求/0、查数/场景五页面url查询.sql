有 -- icon——coad
没有 -- 用 url

select
attributes['WT_ti'],
decodeURLComponent(attributes['WT_event']) as event,
attributes['WT_es']
from olap.event_all 
where toYYYYMMDD(event_time)='20230505'
--and attributes['pageId']= '1585185570189299712'
--and attributes['WT_es'] in ('CA20625','CA17765')
and attributes['WT_es'] like 'https://wap.hi.10086.cn/h5service/h5publish/79YCXHK.html%'
--and event = '0元防骚扰'
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
limit 10;

https://www.sh.10086.cn/h5/server/servlet/autoTokenServlet


0元防骚扰 https://www.sh.10086.cn/h5/server/servlet/autoTokenServlet?busicode=20210528&forwardapp=JTSJYYT&xparam=xopid_1000000002110700006_xorgid_yjdq&WT.ac=20220610_MHYYY_JZ_FL_TC07 CA20625 
0元预约宽带 https://wap.hi.10086.cn/h5service/newestkdzq/broadbandordernew.do?CalLevel3=004&CalLevel2=017&qdbm=1000000002110500001 CA18298 
0元领卡 https://dev.coc.10086.cn/coc/web2/quickreleaseplatformH5/danfaModule?pageId=1473947632963788800&channelId=P00000005570 CA15922 
139邮寄账单 https://h.app.coc.10086.cn/activity/transferXcx/index.html?platform=cmcc&appId=3831652521580768&path=pages%2Fqh_139yjzd%2Findex%2Findex&miniProgramType=0 CA24442 
20GB超值卡 https://wap.hi.10086.cn/h5service/h5publish/59YCXHK.html?CalLevel3=003&CalLevel2=017&qdbm=1000000002110500001 CA22326 
29元10GB https://dx.10086.cn/0J36R3Qr CA17951 
4G短信包 https://wap.cq.10086.cn/mapp/login/ssoservice.html?redirect_protocol=https&redirect_page=https%3A%2F%2Fwap.cq.10086.cn%2Fopenh5%2Fmobilestaticize%2F4GDXB_NEWH5.html%3Fchannel_id%3D70748ED7898ED6CE890BC6282029FE03%2670748ED7898ED6CE890BC6282029FE03%3D6000000134186494 CA21747 
4G飞享78元套餐8折 https://wap.yn.10086.cn/waph52019/card/portalv4/package-4g?channel=jtbdtj CA13893 
4G飞享8折 https://wap.yn.10086.cn/waph52019/card/portalv4/package-4g?channel=GroupAppFLhk CA15043 
5G-sa开关设置 https://api.ahmobile.cn/eip?eip_serv_id=app.h5_5GQA CA25339 
5G套餐 http://wap.gs.10086.cn/gsccwap/olcs/zxsk/toView.html?viewName=zxsk/MobileKing&typeCode=MD_5GP&channel=120&cusClientChannel=zgyd&workNumber=&cityCode=&WT.ac_id=hk_tu3_5Gtc CA11651 
5G套餐原价测试 https://wx.10086.cn/website/customerService/uni?service_ent=e5d53bca72594faea4e8aa5734123ff0&h5new=h5new&service_ext="{"isZGYDAPPmarket":"1","isZGYDAPPwelMsg":"中国移动app个性化欢迎语","isZGYDAPPmarketInfo":{"buttonName":"底部按钮名称","url":