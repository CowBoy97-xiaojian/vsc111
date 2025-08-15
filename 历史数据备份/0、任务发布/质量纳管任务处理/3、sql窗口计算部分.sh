


select
    UNIX_TIMESTAMP(DATE_FORMAT(window_end, 'yyyy-MM-dd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss') `time`,
    count(1) sdk_total,
    count(1) sdk_success_total,
    '100' sdk_avg_rq_time,
    count(1) sdk_pv,
    count(distinct attrs['WT_co_f']) sdk_uv,
    count(distinct COALESCE(attrs['WT_co_f'], attrs['WT_vtvs'])) sdk_vv
FROM TABLE(
    TUMBLE(TABLE TP_DWS_ODZLNG_TIME, DESCRIPTOR(event_tome), INTERVAL '5' SECOND )
)
where attrs['WT_prov'] = '100'
GROUP BY attrs['WT_prov'],window_start, window_end;



select
    wt_cid,
    count(wt_cid)
FROM TABLE(
    TUMBLE(TABLE TP_DWS_ODZLNG_TIME, DESCRIPTOR(event_tome), INTERVAL '5' SECOND )
)
GROUP BY wt_cid,window_start, window_end;

order by wt_cid,dcsdat

遍历窗口中的所有数据

窗口时间
TIMESTAMP_LTZ 


String page,
Long ed,
String mobile,
String cid,
String dcsdat,
String flag,
Long eventTime


WATERMARK FOR complete_dt AS complete_dt – INTERVAL ‘3’ minute ‘proctime’ AS PROCTIME()