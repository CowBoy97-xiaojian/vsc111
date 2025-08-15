set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

insert into table ham_jituan.ads_hachi_cm001 partition (prov='${PROV}')
select
    c_ip,
    wt_cid,
    wt_co_f,
    wt_city,
    wt_mobile,
    wt_event,
    wt_ti,
    wt_ac,
    decode_url(regexp_replace(wt_es,'\\\\n|\\n|\\r|\\\\r','')),
    advertype,
    mark_id,
    trmnl_style,
    wt_av,
    click_time,
    wt_prov,
    channelid,
    plat,
    pageid,
    sellerid
from ham_jituan.dwd_client_event_gio_di where dt='${DT}'
and wt_prov = '${PROV_ID}'
and trmnl_style in ("b95440ef47ec01fc","a1f48d9ff4f42571","b508a809cbbddd0b")
;
