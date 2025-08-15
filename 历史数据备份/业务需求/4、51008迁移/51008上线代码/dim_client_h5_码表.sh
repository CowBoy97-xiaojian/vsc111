CREATE TABLE `dim_client_h5`(
  `pindao` string, 
  `quyu` string, 
  `event` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham_jituan.db/dim_client_h5'
TBLPROPERTIES (
  'transient_lastDdlTime'='1631525313')


#分布式表——本地
CREATE TABLE ham.dim_client_h5_local on cluster cluster_gio_with_shard
(
    pindao      String, 
    quyu        String, 
    event       String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_client_h5_local', '{replica}')
ORDER BY (pindao,quyu,event)
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.dim_client_h5_all on cluster cluster_gio_with_shard
as ham.dim_client_h5_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_client_h5_local', rand());


#插入数据
INSERT INTO ham.dim_client_h5_all (pindao,quyu,event)
values
('首页','轮播图','sy_lb_'),
('首页','ICON功能区','sy_icon_') ,
('分类','分类业务','newfldh_icon_') ,
('分类','分类通栏','newfldh_tlgg_') ,
('新生活','新生活轮播图','2020shy_lb_'),
('新生活','新生活icon','2020shy_icon_') ,
('新生活','新生活营销区域','2020shy_yx_') ,
('个性业务-流量专区','营销专区','llzq_yxlb_') ,
('个性业务-流量专区','娱乐专区','llzq_yllb_') ,
('个性业务--新家庭专区','营销专区','2020bjjtzq_') ,
('个性业务--异网专区','轮播图','yw_sy_lb_') ,
('个性业务--异网专区','icon功能区','yw_sy_icon_') ,
('个性业务--异网专区','营销区域','yw_sy_qy_') ,
('新优惠页','轮播图','2020yh_lb_') ,
('新优惠页','ICON功能区','2020yh_icon_') ,
('新优惠页','营销区域','2020yh_flyxlb_') ,
('新优惠页','页签下内容管理','2020yh_tlqy_') ,
('个性业务-宽带专区','Banner配置','kdyw_lbt_') ,
('个性业务-宽带专区','业务服务区','kdyw_ywlb_') ,
('个性业务-宽带专区','宽带知多少','kdyw_zds_') ,
('通话类功能页','ICON功能区','2020hfzd_icon_') ,
('首页','新营销区域','sy_qy_'),
('个性业务-流量专区','ICON功能区','llzq_iocn_');