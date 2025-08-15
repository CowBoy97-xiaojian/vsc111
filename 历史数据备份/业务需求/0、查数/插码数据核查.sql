select 
attributes['WT_et'] as et,
decodeURLComponent(attributes['WT_event']) as event,
decodeURLComponent(attributes['WT_envName']) as envName,
attributes['WT_goods_id'] as goodsId,
decodeURLComponent(attributes['WT_si_n']) as sin,
decodeURLComponent(attributes['WT_si_x']) as six,
attributes['WT_errCode'] as errCode,
decodeURLComponent(attributes['WT_errMsg']) as errMsg,
decodeURLComponent(attributes['WT_es']) as es
from olap.event_all 
where toYYYYMMDD(event_time)='20230505'
and es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-7daypkg/index.html%'
--and attributes['pageId']= '1585185570189299712'
and et = 'clk' and  six in ('20','21')
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
limit 10;


select 
attributes['WT_et'] as et,
decodeURLComponent(attributes['WT_event']) as event,
decodeURLComponent(attributes['WT_envName']) as envName,
attributes['WT_goods_id'] as goodsId,
decodeURLComponent(attributes['WT_si_n']) as sin,
decodeURLComponent(attributes['WT_si_x']) as six,
attributes['WT_errCode'] as errCode,
decodeURLComponent(attributes['WT_errMsg']) as errMsg,
decodeURLComponent(attributes['WT_es']) as es
from olap.event_all 
where toYYYYMMDD(event_time)='20230505'
and decodeURLComponent(attributes['WT_es']) like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-7daypkg/index.html%'
--and attributes['pageId']= '1651119741764055040'
and et = 'ordst' and  six in ('99','-99')
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
limit 10;



select 
attributes['WT_et'] as et,
decodeURLComponent(attributes['WT_event']) as event,
decodeURLComponent(attributes['WT_envName']) as envName,
attributes['WT_goods_id'] as goodsId,
decodeURLComponent(attributes['WT_si_n']) as sin,
decodeURLComponent(attributes['WT_si_x']) as six,
attributes['WT_errCode'] as errCode,
decodeURLComponent(attributes['WT_errMsg']) as errMsg,
decodeURLComponent(attributes['WT_es']) as es
from olap.event_all 
where toYYYYMMDD(event_time)='20230505'
and es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-7daypkg/index.html%'
--and attributes['pageId']= '1651119741764055040'
and et = 'pageview' and  event ='H5PageShow'
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
limit 10;









select count(1) from event_all where toYYYYMMDD(event_time)='20230521' and toHour(event_time)='6' and data_source_id in ('90be4403373b6463','dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','b508a809cbbddd0b','dcs0cxkozfonmgrs8gfnw57g1_2e4p','a1f48d9ff4f42571','dcs47gbrugonmg3u1x8njabg1_2p4f','b95440ef47ec01fc');






select 
attributes['WT_et'] as et,
decodeURLComponent(attributes['WT_event']) as event,
decodeURLComponent(attributes['WT_envName']) as envName,
attributes['WT_goods_id'] as goodsId,
decodeURLComponent(attributes['WT_si_n']) as sin,
decodeURLComponent(attributes['WT_si_x']) as six,
attributes['WT_errCode'] as errCode,
decodeURLComponent(attributes['WT_errMsg']) as errMsg,
decodeURLComponent(attributes['WT_es']) as es
from olap.event_all 
where toYYYYMMDD(event_time)='20230505'
and attributes['pageId']= '1540146148781355008'
and et = 'clk' and  six in ('20','21')
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
limit 10;


select 
attributes['WT_et'] as et,
decodeURLComponent(attributes['WT_event']) as event,
decodeURLComponent(attributes['WT_envName']) as envName,
attributes['WT_goods_id'] as goodsId,
decodeURLComponent(attributes['WT_si_n']) as sin,
decodeURLComponent(attributes['WT_si_x']) as six,
attributes['WT_errCode'] as errCode,
decodeURLComponent(attributes['WT_errMsg']) as errMsg,
decodeURLComponent(attributes['WT_es']) as es
from olap.event_all 
where toYYYYMMDD(event_time)='20230505'
--and attributes['pageId']= '1651119741764055040'
and et = 'ordst' and  six in ('99','-99')
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
limit 10;



select 
attributes['WT_et'] as et,
decodeURLComponent(attributes['WT_event']) as event,
decodeURLComponent(attributes['WT_envName']) as envName,
attributes['WT_goods_id'] as goodsId,
decodeURLComponent(attributes['WT_si_n']) as sin,
decodeURLComponent(attributes['WT_si_x']) as six,
attributes['WT_errCode'] as errCode,
decodeURLComponent(attributes['WT_errMsg']) as errMsg,
decodeURLComponent(attributes['WT_es']) as es
--data_source_id
from olap.event_all 
where toYYYYMMDD(event_time)='20230505'
--and attributes['pageId']= '1651119741764055040'
and et = 'pageview' and  event ='H5PageShow'
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
limit 10;


博新 pageId

select 
attributes['pageId'] pageId
from olap.event_all 
where toYYYYMMDD(event_time)='20230505'
and attributes['pageId'] in ('1319879972513357824','1386587330419818496','1320571046785298432','1512269268765949952','1583025552892329984','1531108938734608384','1528576199134941184','1585179904280186880','1535074831321169920','1549667275928436736','1560515556860538880','1571669917221302272','1572162871747760128','1519238383930933248','1519237071554273280','1530077833449897984','1530078608888143872','1530078906327695360','1530079332192157696','1583027039097815040','1583026425218494464','1583027688698048512','1583028214529552384','1538797930025873408','1540146148781355008','1333582930123542528','1585185570189299712','1303527939678859264','1303528106117230592','1610561438253711360','1532257384428081152','1571669917221302272','1572162871747760128','1519238383930933248','1519237071554273280','1304220140565278720','1570677358659665920','1524573589774991360','1568151954003582976','1582287186043305984','1560566628382756864','1570678482906112000','1648967009530159104','1640647220644003840','1643169841860296704','1635574395914665984','1635579003168866304','1638000397142581248','1654737960745623552','1651119741764055040')
and attributes['WT_et'] = 'clk' and  decodeURLComponent(attributes['WT_si_x']) in ('20','21')
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
group by pageId
limit 10;

select 
attributes['pageId'] pageId
from olap.event_all 
where toYYYYMMDD(event_time)='20230505'
and attributes['pageId'] in ('1319879972513357824','1386587330419818496','1320571046785298432','1512269268765949952','1583025552892329984','1531108938734608384','1528576199134941184','1585179904280186880','1535074831321169920','1549667275928436736','1560515556860538880','1571669917221302272','1572162871747760128','1519238383930933248','1519237071554273280','1530077833449897984','1530078608888143872','1530078906327695360','1530079332192157696','1583027039097815040','1583026425218494464','1583027688698048512','1583028214529552384','1538797930025873408','1540146148781355008','1333582930123542528','1585185570189299712','1303527939678859264','1303528106117230592','1610561438253711360','1532257384428081152','1571669917221302272','1572162871747760128','1519238383930933248','1519237071554273280','1304220140565278720','1570677358659665920','1524573589774991360','1568151954003582976','1582287186043305984','1560566628382756864','1570678482906112000','1648967009530159104','1640647220644003840','1643169841860296704','1635574395914665984','1635579003168866304','1638000397142581248','1654737960745623552','1651119741764055040')
and attributes['WT_et'] = 'ordst' and  decodeURLComponent(attributes['WT_si_x']) in ('99','-99')
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
group by pageId
limit 10;



select 
attributes['pageId'] pageId
from olap.event_all 
where toYYYYMMDD(event_time)='20230505'
and attributes['pageId'] in ('1319879972513357824','1386587330419818496','1320571046785298432','1512269268765949952','1583025552892329984','1531108938734608384','1528576199134941184','1585179904280186880','1535074831321169920','1549667275928436736','1560515556860538880','1571669917221302272','1572162871747760128','1519238383930933248','1519237071554273280','1530077833449897984','1530078608888143872','1530078906327695360','1530079332192157696','1583027039097815040','1583026425218494464','1583027688698048512','1583028214529552384','1538797930025873408','1540146148781355008','1333582930123542528','1585185570189299712','1303527939678859264','1303528106117230592','1610561438253711360','1532257384428081152','1571669917221302272','1572162871747760128','1519238383930933248','1519237071554273280','1304220140565278720','1570677358659665920','1524573589774991360','1568151954003582976','1582287186043305984','1560566628382756864','1570678482906112000','1648967009530159104','1640647220644003840','1643169841860296704','1635574395914665984','1635579003168866304','1638000397142581248','1654737960745623552','1651119741764055040')
and attributes['WT_et'] = 'pageview' and  decodeURLComponent(attributes['WT_event']) ='H5PageShow'
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
group by pageId
limit 10;




5-18
1540146148781355008

1538797930025873408
1540146148781355008


1538797930025873408
1540146148781355008

splitByString('?',es)


博新URL
select 
splitByString('?',ifnull(decodeURLComponent(attributes['WT_es']),''))[1] as es
from olap.event_all 
where toYYYYMMDD(event_time)='20230518'
and attributes['WT_et'] = 'clk' and  decodeURLComponent(attributes['WT_si_x']) in ('20','21')
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
and (es like '%dev.coc.10086.cn/coc2/web-depass/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/ptvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-degvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-composition-order/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4zone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-7daypkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxzone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-welfare4follow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4qq/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4walk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-aqysxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-txsxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-yksxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-mgsxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/preferrednumber/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/5gpackage/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/smartpersonalpkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/discountpacket/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-mengcard-newyear/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-cutecard/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/rpflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-7daypkgdouble/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-filialpkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-lowflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-d11lowflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/Ktb-web-pkgoriginal/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-flowpkg4day/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-flowpkg20/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-douyinringtone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-douyinringtoneorignal/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-clouddiskvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguvideovip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguringtone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguringnew/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguaggr/index.html%')
group by es;



select 
b.et,
b.event,
b.envName,
b.goodsId,
b.sin,
b.six,
b.errCode,
b.errMsg,
b.es1,
b.es
from
(select 
splitByString('?',ifnull(decodeURLComponent(attributes['WT_es']),''))[1] as es
from olap.event_all 
where toYYYYMMDD(event_time)='20230518'
--and attributes['WT_et'] = 'clk' and  decodeURLComponent(attributes['WT_si_x']) in ('20','21')
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
and (es like '%dev.coc.10086.cn/coc2/web-depass/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/ptvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-degvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-composition-order/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4zone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-7daypkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxzone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-welfare4follow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4qq/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4walk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-aqysxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-txsxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-yksxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-mgsxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/preferrednumber/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/5gpackage/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/smartpersonalpkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/discountpacket/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-mengcard-newyear/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-cutecard/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/rpflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-7daypkgdouble/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-filialpkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-lowflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-d11lowflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/Ktb-web-pkgoriginal/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-flowpkg4day/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-flowpkg20/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-douyinringtone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-douyinringtoneorignal/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-clouddiskvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguvideovip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguringtone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguringnew/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguaggr/index.html%')
group by es) a 
left join 
(
select 
attributes['WT_et'] as et,
decodeURLComponent(attributes['WT_event']) as event,
decodeURLComponent(attributes['WT_envName']) as envName,
attributes['WT_goods_id'] as goodsId,
decodeURLComponent(attributes['WT_si_n']) as sin,
decodeURLComponent(attributes['WT_si_x']) as six,
attributes['WT_errCode'] as errCode,
decodeURLComponent(attributes['WT_errMsg']) as errMsg,
decodeURLComponent(attributes['WT_es']) as es1,
splitByString('?',ifnull(decodeURLComponent(attributes['WT_es']),''))[1] as es
from olap.event_all 
where toYYYYMMDD(event_time)='20230518'
) b
on a.es = b.es;



gio URL


select 
decodeURLComponent(attributes['WT_es']) as es
from olap.event_all 
where toYYYYMMDD(event_time)='20230507'
and attributes['WT_et'] = 'ordst' and  decodeURLComponent(attributes['WT_si_x']) in ('99','-99')
and data_source_id in ('dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','dcs0cxkozfonmgrs8gfnw57g1_2e4p','dcs47gbrugonmg3u1x8njabg1_2p4f')
and (es like '%dev.coc.10086.cn/coc2/web-depass/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/ptvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-degvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-composition-order/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4zone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-7daypkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxzone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-welfare4follow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4qq/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4walk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-aqysxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-txsxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-yksxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-mgsxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/preferrednumber/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/5gpackage/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/smartpersonalpkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/discountpacket/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-mengcard-newyear/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-cutecard/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/rpflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-7daypkgdouble/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-filialpkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-lowflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-d11lowflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/Ktb-web-pkgoriginal/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-flowpkg4day/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-flowpkg20/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-douyinringtone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-douyinringtoneorignal/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-clouddiskvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguvideovip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguringtone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguringnew/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguaggr/index.html%')
group by es;






select 
if(position(domain,'.')>0,if(match(domain,'^[0-9]+[.]+[0-9]+[.]+[0-9]+[.]+[0-9]+$'),concat('http://',ifNull(domain,''),ifNull(attributes['$path'],'')),concat('',ifNull(domain,''),ifNull(attributes['$path'],''))),domain) as es
from olap.event_all 
where toYYYYMMDD(event_time)='20230518'
and es is not null
and attributes['WT_et'] = 'pageview' and  decodeURLComponent(attributes['WT_event']) ='H5PageShow'
and data_source_id in ('90be4403373b6463','dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','b508a809cbbddd0b','dcs0cxkozfonmgrs8gfnw57g1_2e4p','a1f48d9ff4f42571','dcs47gbrugonmg3u1x8njabg1_2p4f','b95440ef47ec01fc')
and (es like '%dev.coc.10086.cn/coc2/web-depass/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/ptvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-degvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-composition-order/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4zone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxk-7daypkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-sxzone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-welfare4follow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4qq/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4walk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-pkg4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-aqysxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-txsxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-yksxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-mgsxk-original/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/preferrednumber/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/5gpackage/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/smartpersonalpkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/discountpacket/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-mengcard-newyear/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-cutecard/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4aqy/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-penny4tx/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4yk/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-follow4mg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao/rpflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-7daypkgdouble/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-filialpkg/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-lowflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-d11lowflow/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/Ktb-web-pkgoriginal/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-flowpkg4day/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-flowpkg20/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-douyinringtone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-douyinringtoneorignal/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-clouddiskvip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguvideovip/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguringtone/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguringnew/index.html%'
or es like '%dev.coc.10086.cn/coc2/web-kataobao2/ktb-web-miguaggr/index.html%')
group by es;





gio

select
decodeURLComponent(attributes['$query']),
substr(nullif(extract(decodeURLComponent(attributes['$query']), 'pageId=([0-9a-zA-z]+)'),''), 1, 19) as pageId
from olap.event_all 
where toYYYYMMDD(event_time)='20230505'
--and attributes['pageId'] in ('1319879972513357824','1386587330419818496','1320571046785298432','1512269268765949952','1583025552892329984','1531108938734608384','1528576199134941184','1585179904280186880','1535074831321169920','1549667275928436736','1560515556860538880','1571669917221302272','1572162871747760128','1519238383930933248','1519237071554273280','1530077833449897984','1530078608888143872','1530078906327695360','1530079332192157696','1583027039097815040','1583026425218494464','1583027688698048512','1583028214529552384','1538797930025873408','1540146148781355008','1333582930123542528','1585185570189299712','1303527939678859264','1303528106117230592','1610561438253711360','1532257384428081152','1571669917221302272','1572162871747760128','1519238383930933248','1519237071554273280','1304220140565278720','1570677358659665920','1524573589774991360','1568151954003582976','1582287186043305984','1560566628382756864','1570678482906112000','1648967009530159104','1640647220644003840','1643169841860296704','1635574395914665984','1635579003168866304','1638000397142581248','1654737960745623552','1651119741764055040')
--and attributes['WT_et'] = 'clk' and  decodeURLComponent(attributes['WT_si_x']) in ('20','21')
and data_source_id in ('90be4403373b6463','b508a809cbbddd0b','a1f48d9ff4f42571','b95440ef47ec01fc')
and attributes['$query'] is not null
--group by pageId
limit 10;





select count(1) from event_all where toYYYYMMDD(event_time)='20230521' and toHour(event_time)='6' and data_source_id in ('90be4403373b6463','dcs4311adgonmgj23shb4oqyy_5q5k','dcsgswzxehonmgrc8hz5w67g1_9o7q','b508a809cbbddd0b','dcs0cxkozfonmgrs8gfnw57g1_2e4p','a1f48d9ff4f42571','dcs47gbrugonmg3u1x8njabg1_2p4f','b95440ef47ec01fc');