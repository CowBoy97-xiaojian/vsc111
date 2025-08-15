
set hive.default.fileformat = Orc;
set hive.execution.engine = mr;


insert overwrite table ham.dim_rpt_hachi_domain partition (interface = '51010')
select parse_url(decode_url(query['WT.es']),'HOST') as domain
from ham.ods_dcslog_delta 
where dt between date_sub(CAST('${DT}' AS timestamp), 7) and '${DT}'
and parse_url(decode_url(query['WT.es']),'HOST') is not null
and dcsid in ('7c2d','3s3i','7p1j','3a5k','8z2s','1w7q','9w6z','8f4q','2i1b','6g7b','0d4d','8d2o','8m4a','5d3k','3z9h','4w6p','5f8e','3j1d','9o0p','0o1j','3z7i','9b5g','3r8r','3d2m','9m2x','8o8q','2v6r','8j5v','4z6i','9y7c','8s0b','1g3e','3n9u','6c9w','2n3t')
group by parse_url(decode_url(query['WT.es']),'HOST')
;

insert into table ham.dim_rpt_hachi_domain partition (interface = '51010')
select domain
from ham.ods_dcslog_event_gio_all 
where dt between date_sub(CAST('${DT}' AS timestamp), 7) and '${DT}'
and domain is not null
and data_source_id in ('a797613d647af6c6','a7464be8b200fe24','b9e74b9d5da046ce','b87eb8cec01051a6','b098587a3af15a25','b23efeab1461679b','a10b1f67c7d288c1','ae2308d5a08258b6','a04f0ad9925c080f','a6ba03d6884ee1c1','aa3a7b170aa41eed','a441e7427a44d390','adae9b8274a853f1','ad2e89b1010aed1e','bfe72e945deea64f','81ebc780373ee7cd','8d5d4407d1c3f6bf','abd580f08cf381e0','ba81a7647a8bed5d','8c19b47f237e28d3','a609de73d1690c0e','9c294cf845907ac1','b0839305cf5d280d','9a238b92b6612604','afdbd0f6b9a7ba30','8273883304ad70fb','86fdf7160c5cc8cc','81fab8d01c17ee89','89e7161d4a6b817f','9b50668a016469ec','bda85eeb52fb8b87','a8c29f7abfd639b4','b8d9fd437de471e4','875444ff6e049f33','853d176d98834573')
group by domain
;

