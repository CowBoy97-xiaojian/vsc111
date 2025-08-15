set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

insert overwrite table ham_jituan.ads_hachi_cm001_shanghai partition (prov='${PROV}')
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
    sellerid,
    et,
    envname,
    next_url,
    si_x,
    si_n,
    sku_id,
    goods_id,
    errCode,
    errmsg,
    si_s,
    wt_mc_id,
    vt_sid
from ham_jituan.dwd_client_event_di where dt='${DT}'
and wt_prov = '${PROV_ID}'
and trmnl_style in ('H5','IOS','ANDROID')
;
