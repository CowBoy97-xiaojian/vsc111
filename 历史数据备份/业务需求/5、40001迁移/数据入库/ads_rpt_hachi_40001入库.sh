createtab_stmt
CREATE TABLE `ham.ads_rpt_hachi_40001`(
  `daytime` string COMMENT 'from deserializer', 
  `ip` string COMMENT 'from deserializer', 
  `channel_id` string COMMENT 'from deserializer', 
  `page_id` string COMMENT 'from deserializer', 
  `seller_id` string COMMENT 'from deserializer', 
  `wtdcsdat` string COMMENT 'from deserializer', 
  `wtchannelid` string COMMENT 'from deserializer', 
  `wtco_f` string COMMENT 'from deserializer', 
  `wtevent` string COMMENT 'from deserializer', 
  `wturl` string COMMENT 'from deserializer', 
  `wtmc_ev` string COMMENT 'from deserializer', 
  `wtsellerid` string COMMENT 'from deserializer', 
  `wtpageid` string COMMENT 'from deserializer', 
  `wtlast_pageid` string COMMENT 'from deserializer', 
  `wtlast_url` string COMMENT 'from deserializer', 
  `wtlast_pagename` string COMMENT 'from deserializer', 
  `wtcurrent_url` string COMMENT 'from deserializer', 
  `wtcurrent_pagename` string COMMENT 'from deserializer', 
  `wtmodule_no` string COMMENT 'from deserializer', 
  `wtmodule_name` string COMMENT 'from deserializer', 
  `wtpoint_position` string COMMENT 'from deserializer', 
  `wtmember` string COMMENT 'from deserializer', 
  `wtgoods_no` string COMMENT 'from deserializer', 
  `wtlogin_status` string COMMENT 'from deserializer', 
  `wtnext_pageid` string COMMENT 'from deserializer', 
  `wtnext_url` string COMMENT 'from deserializer', 
  `wtnext_pagename` string COMMENT 'from deserializer', 
  `wtcomponent_id` string COMMENT 'from deserializer', 
  `user_agent` string COMMENT 'from deserializer', 
  `referer` string COMMENT 'from deserializer', 
  `mobile` string COMMENT 'from deserializer', 
  `session_id` string COMMENT 'from deserializer', 
  `wtinput_sfz` string COMMENT 'from deserializer', 
  `environment` string COMMENT 'from deserializer', 
  `ip_prov` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `code` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='||', 
  'serialization.null.format'='-') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/ads_rpt_hachi_40001'
TBLPROPERTIES (
  'transient_lastDdlTime'='1634024066')






#分布式表——本地
CREATE TABLE ham.ads_rpt_hachi_40001_local on cluster cluster_gio_with_shard
(
    daytime Nullable(String), 
    ip Nullable(String), 
    channel_id Nullable(String), 
    page_id Nullable(String), 
    seller_id Nullable(String), 
    wtdcsdat Nullable(String), 
    wtchannelid Nullable(String), 
    wtco_f Nullable(String), 
    wtevent Nullable(String), 
    wturl Nullable(String), 
    wtmc_ev Nullable(String), 
    wtsellerid Nullable(String), 
    wtpageid Nullable(String), 
    wtlast_pageid Nullable(String), 
    wtlast_url Nullable(String), 
    wtlast_pagename Nullable(String), 
    wtcurrent_url Nullable(String), 
    wtcurrent_pagename Nullable(String), 
    wtmodule_no Nullable(String), 
    wtmodule_name Nullable(String), 
    wtpoint_position Nullable(String), 
    wtmember Nullable(String), 
    wtgoods_no Nullable(String), 
    wtlogin_status Nullable(String), 
    wtnext_pageid Nullable(String), 
    wtnext_url Nullable(String), 
    wtnext_pagename Nullable(String), 
    wtcomponent_id Nullable(String), 
    user_agent Nullable(String), 
    referer Nullable(String), 
    mobile Nullable(String), 
    session_id Nullable(String), 
    wtinput_sfz Nullable(String), 
    environment Nullable(String), 
    ip_prov Nullable(String),
    code String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.ads_rpt_hachi_40001_local', '{replica}')
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/webtrends.event_hi_client_local', '{replica}')
PARTITION BY code
ORDER BY code
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';



#分布式表——视图
CREATE TABLE ham.ads_rpt_hachi_40001_all on cluster cluster_gio_with_shard
as ham.ads_rpt_hachi_40001_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'ads_rpt_hachi_40001_local', rand());

OPTIMIZE table ham.ads_rpt_hachi_40001_local on cluster cluster_gio_with_shard;