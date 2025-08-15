--51010逻辑


insert overwrite table ads_rpt_hachi_domain
select domain
from olap.event_all
where toDate(event_time) between '2023-07-13' and '2023-07-19'  --运行调度前一周
and data_source_id in ('aa410d5cd21666f5','a1307114a76cd375','a6381779603b656a','913e6dc4915d470c','8643086d472dea68','82213179b9aea392','aba9de4ce446b2d2')  --51006渠道
and match(attributes['WT_mc_ev'],'QYCS')=0

{"3w7x", "5m9b", "7e9m", "5i5n", "2r9o", "4t5g", "9o4w", "3l2h", "2p4f", "2e4p", "9o7q", "5q5k", "7c2d", "3s3i", "7p1j", "3a5k", "8z2s", "1w7q", "9w6z", "8f4q", "2i1b", "6g7b", "0d4d", "8d2o", "8m4a", "5d3k", "3z9h", "4w6p", "5f8e", "3j1d", "9o0p", "0o1j", "3z7i", "9b5g", "3r8r", "3d2m", "9m2x", "8o8q", "2v6r", "8j5v", "4z6i", "9y7c", "8s0b", "1g3e", "7i1l", "5x7m", "3x6g", "5n4c", "4i1j", "5o9d", "8c6a", "6w4k", "6i1f", "4d2v", "9c7w", "4t8n", "2x6l", "7g6o", "8h7d", "9g3l", "6w5o", "4j7p", "6p5i", "4q1j", "1a1y", "6m4s", "2c8g", "7y1o", "4d5v", "5x8u", "3b6u", "7e5j", "2k2c", "4d2r", "8k7f", "3n9u", "6c9w", "3p3y", "8g8r", "2n3t", "4f1d", "9i1s", "8q9w", "2k2s", "3x2l"}


--51006逻辑（排除51010domain数据）
select domain
from olap.event_all
where toDate(event_time) between '2023-07-13' and '2023-07-19'  --运行调度前一周
and data_source_id in ('a797613d647af6c6','a7464be8b200fe24','b9e74b9d5da046ce','b87eb8cec01051a6','b098587a3af15a25','b23efeab1461679b','a10b1f67c7d288c1','ae2308d5a08258b6','a04f0ad9925c080f','a6ba03d6884ee1c1','aa3a7b170aa41eed','a441e7427a44d390','adae9b8274a853f1','ad2e89b1010aed1e','bfe72e945deea64f','81ebc780373ee7cd','8d5d4407d1c3f6bf','abd580f08cf381e0','ba81a7647a8bed5d','8c19b47f237e28d3','a609de73d1690c0e','9c294cf845907ac1','b0839305cf5d280d','9a238b92b6612604','afdbd0f6b9a7ba30','8273883304ad70fb','86fdf7160c5cc8cc','81fab8d01c17ee89','89e7161d4a6b817f','9b50668a016469ec','bda85eeb52fb8b87','a8c29f7abfd639b4','b8d9fd437de471e4','875444ff6e049f33','853d176d98834573');

原生  is not null  wt_av;  

'3w7x','5m9b','7e9m','5i5n','2r9o','4t5g','9o4w','3l2h'

select parse_url(decode_url(query['WT.es']),'HOST')
from ods_dcslog_delta 
where dt = '2023-07-19' and hour = '01'
and dcsid in ('3w7x','5m9b','7e9m','5i5n','2r9o','4t5g','9o4w','3l2h')
and instr(query['WT.mc_ev'],'QYCS') > 0
limit 1;


('7c2d','3s3i','7p1j','3a5k','8z2s','1w7q','9w6z','8f4q','2i1b','6g7b','0d4d','8d2o','8m4a','5d3k','3z9h','4w6p','5f8e','3j1d','9o0p','0o1j','3z7i','9b5g','3r8r','3d2m','9m2x','8o8q','2v6r','8j5v','4z6i','9y7c','8s0b','1g3e','3n9u','6c9w','2n3t')

select parse_url(decode_url(query['WT.es']),'HOST') as domain
from ods_dcslog_delta 
where dt = '2023-07-19' and hour = '01'
and dcsid in ('7c2d','3s3i','7p1j','3a5k','8z2s','1w7q','9w6z','8f4q','2i1b','6g7b','0d4d','8d2o','8m4a','5d3k','3z9h','4w6p','5f8e','3j1d','9o0p','0o1j','3z7i','9b5g','3r8r','3d2m','9m2x','8o8q','2v6r','8j5v','4z6i','9y7c','8s0b','1g3e','3n9u','6c9w','2n3t')
group by parse_url(decode_url(query['WT.es']),'HOST')
limit 10
;

insert overwrite table ham.dim_rpt_hachi_domain partition (interface = '51006')
select parse_url(decode_url(query['WT.es']),'HOST') as domain
from ods_dcslog_delta 
where dt between '2023-07-13' and '2023-07-19'
and parse_url(decode_url(query['WT.es']),'HOST') is not null
and dcsid in ('7c2d','3s3i','7p1j','3a5k','8z2s','1w7q','9w6z','8f4q','2i1b','6g7b','0d4d','8d2o','8m4a','5d3k','3z9h','4w6p','5f8e','3j1d','9o0p','0o1j','3z7i','9b5g','3r8r','3d2m','9m2x','8o8q','2v6r','8j5v','4z6i','9y7c','8s0b','1g3e','3n9u','6c9w','2n3t')
group by parse_url(decode_url(query['WT.es']),'HOST')
limit 20
;