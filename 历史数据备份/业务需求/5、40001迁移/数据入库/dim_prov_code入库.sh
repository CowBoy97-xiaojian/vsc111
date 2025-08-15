上海市,10200,上海
云南省,12900,云南
全球,99999,全球
内蒙古自治区,12800,内蒙古
北京市,10100,北京
台湾省,99999,台湾
吉林省,11800,吉林
四川省,13000,四川
天津市,10300,天津
宁夏回族自治区,11900,宁夏
安徽省,11000,安徽
山东省,12000,山东
山西省,12100,山西
广东省,12300,广东
广西壮族自治区,12500,广西
新疆维吾尔自治区,12200,新疆
江苏省,11700,江苏
江西省,12700,江西
河北省,10800,河北
河南省,10900,河南
浙江省,11400,浙江
海南省,11500,海南
湖北省,10600,湖北
湖南省,12600,湖南
澳门特别行政区,99999,澳门
甘肃省,11300,甘肃
福建省,11100,福建
西藏自治区,13100,西藏
贵州省,10500,贵州
辽宁省,12400,辽宁
重庆市,10400,重庆
陕西省,10700,陕西
青海省,11200,青海
香港特别行政区,99999,香港
黑龙江省,11600,黑龙江


#hive表
createtab_stmt
CREATE TABLE `dim_prov_code`(
  `ip_prov` string, 
  `code` string, 
  `short_name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://udbachdp1/user/hive/warehouse/ham.db/dim_prov_code'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'last_modified_by'='udbac', 
  'last_modified_time'='1668583429', 
  'numFiles'='1', 
  'numRows'='-1', 
  'rawDataSize'='-1', 
  'totalSize'='638', 
  'transient_lastDdlTime'='1668583429')


#分布式表——本地
CREATE TABLE ham.dim_prov_code_local on cluster cluster_gio_with_shard
(
    ip_prov         String, 
    code            String, 
    short_name      String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_prov_code_local', '{replica}')
ORDER BY code
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';


#分布式表——视图
CREATE TABLE ham.dim_prov_code_all on cluster cluster_gio_with_shard
as ham.dim_prov_code_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_prov_code_local', rand());


INSERT INTO ham.dim_prov_code_all (ip_prov,code,short_name)
values
('上海市','10200','上海'),
('云南省','12900','云南'),
('全球','99999','全球'),
('内蒙古自治区','12800','内蒙古'),
('北京市','10100','北京'),
('台湾省','99999','台湾'),
('吉林省','11800','吉林'),
('四川省','13000','四川'),
('天津市','10300','天津'),
('宁夏回族自治区','11900','宁夏'),
('安徽省','11000','安徽'),
('山东省','12000','山东'),
('山西省','12100','山西'),
('广东省','12300','广东'),
('广西壮族自治区','12500','广西'),
('新疆维吾尔自治区','12200','新疆'),
('江苏省','11700','江苏'),
('江西省','12700','江西'),
('河北省','10800','河北'),
('河南省','10900','河南'),
('浙江省','11400','浙江'),
('海南省','11500','海南'),
('湖北省','10600','湖北'),
('湖南省','12600','湖南'),
('澳门特别行政区','99999','澳门'),
('甘肃省','11300','甘肃'),
('福建省','11100','福建'),
('西藏自治区','13100','西藏'),
('贵州省','10500','贵州'),
('辽宁省','12400','辽宁'),
('重庆市','10400','重庆'),
('陕西省','10700','陕西'),
('青海省','11200','青海'),
('香港特别行政区','99999','香港'),
('黑龙江省','11600','黑龙江');