

100 - detail


CREATE TABLE ham.dim_order_detail_local on cluster cluster_gio_with_shard
(
    create_date String,
    province_name String,
    page_id String,
    touch_code String,
    touch_name String,
    seller_id String,
    goods_id String,
    goods_name String,
    order_type String,
    sales int,
    dt String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_detail_local', '{replica}')
PARTITION BY dt
ORDER BY dt
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.dim_order_detail_all on cluster cluster_gio_with_shard
as ham.dim_order_detail_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_detail_local', rand());


临时表
CREATE TABLE ham.dim_order_detail_tmp_local on cluster cluster_gio_with_shard
(
    create_date String,
    province_name String,
    page_id String,
    touch_code String,
    touch_name String,
    seller_id String,
    goods_id String,
    goods_name String,
    order_type String,
    sales int
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_detail_tmp_local', '{replica}')
ORDER BY create_date
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.dim_order_detail_tmp_all on cluster cluster_gio_with_shard
as ham.dim_order_detail_tmp_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_detail_tmp_local', rand());


create_date,province_name,page_id,touch_code,touch_name,seller_id,goods_id,goods_name,order_type,sales,dt




101 - activity

CREATE TABLE ham.dim_order_activity_local on cluster cluster_gio_with_shard
(
    create_date String,
    province_name String,
    channel_id String,
    channel_name String,
    seller_id String,
    activity_name String,
    sku_code String,
    sku_name String,
    sales int,
    dt String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_activity_local', '{replica}')
PARTITION BY dt
ORDER BY dt
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.dim_order_activity_all on cluster cluster_gio_with_shard
as ham.dim_order_activity_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_activity_local', rand());


临时表

CREATE TABLE ham.dim_order_activity_tmp_local on cluster cluster_gio_with_shard
(
create_date String,
province_name String,
channel_id String,
channel_name String,
seller_id String,
activity_name String,
sku_code String,
sku_name String,
sales int
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_activity_tmp_local', '{replica}')
ORDER BY create_date
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.dim_order_activity_tmp_all on cluster cluster_gio_with_shard
as ham.dim_order_activity_tmp_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_activity_tmp_local', rand());


create_date,province_name,channel_id,channel_name,seller_id,activity_name,sku_code,sku_name,sales,dt


200 - channel

CREATE TABLE ham.dim_order_channel_local on cluster cluster_gio_with_shard
(
    create_date String,
    channel_id_l4 String,
    channel_id_l3 String,
    channel_id_l2 String,
    channel_id_l1 String,
    channel_name_l4 String,
    channel_name_l3 String,
    channel_name_l2 String,
    channel_name_l1 String,
    channel_type String,
    dt String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_channel_local', '{replica}')
PARTITION BY dt
ORDER BY dt
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.dim_order_channel_all on cluster cluster_gio_with_shard
as ham.dim_order_channel_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_channel_local', rand());


临时表

CREATE TABLE ham.dim_order_channel_tmp_local on cluster cluster_gio_with_shard
(
create_date String,
channel_id_l4 String,
channel_id_l3 String,
channel_id_l2 String,
channel_id_l1 String,
channel_name_l4 String,
channel_name_l3 String,
channel_name_l2 String,
channel_name_l1 String,
channel_type String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_channel_tmp_local', '{replica}')
PARTITION BY create_date
ORDER BY create_date
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.dim_order_channel_tmp_all on cluster cluster_gio_with_shard
as ham.dim_order_channel_tmp_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_channel_tmp_local', rand());


create_date,channel_id_l4,channel_id_l3,channel_id_l2,channel_id_l1,channel_name_l4,channel_name_l3,channel_name_l2,channel_name_l1,channel_type,dt



201 page

CREATE TABLE ham.dim_order_page_tmp_local on cluster cluster_gio_with_shard
(
statis_date String,
activity_type String,
page_id String,
page_type String,
page_name String,
app_name String,
channel_id String,
url String,
url_status String,
start_time String,
end_time String
)   
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_page_tmp_local', '{replica}')
ORDER BY page_id
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';

CREATE TABLE ham.dim_order_page_tmp_all on cluster cluster_gio_with_shard
as ham.dim_order_page_tmp_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_page_tmp_local', rand());


CREATE TABLE ham.dim_order_page_local on cluster cluster_gio_with_shard
(
statis_date String,
activity_type String,
page_id String,
page_type String,
page_name String,
app_name String,
channel_id String,
url String,
url_status String,
start_time String,
end_time String,
dt String
)   
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_page_local', '{replica}')
PARTITION BY dt
ORDER BY dt
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk';
  
CREATE TABLE ham.dim_order_page_all on cluster cluster_gio_with_shard
as ham.dim_order_page_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_page_local', rand());


202 - product

CREATE TABLE ham.dim_order_product_local on cluster cluster_gio_with_shard
(
statis_date String,
activity_type String,
page_id String,
product_id String,
goods_id String,
goods_name String,
activity_id String,
dt String
)   
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_product_local', '{replica}')
PARTITION BY dt
ORDER BY dt
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk'; 
  
CREATE TABLE ham.dim_order_product_all on cluster cluster_gio_with_shard
as ham.dim_order_product_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_product_local', rand());


CREATE TABLE ham.dim_order_product_tmp_local on cluster cluster_gio_with_shard
(
    statis_date String,
    activity_type String,
    page_id String,
    product_id String,
    goods_id String,
    goods_name String,
    activity_id String
)   
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_order_product_tmp_local', '{replica}')
ORDER BY activity_id
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk'; 

CREATE TABLE ham.dim_order_product_tmp_all on cluster cluster_gio_with_shard
as ham.dim_order_product_tmp_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_order_product_tmp_local', rand());



QYCS_EventTrackingMapping


CREATE TABLE ham.dim_qycs_component_local on cluster cluster_gio_with_shard
(
    component_id String,
    component_name String,
    point_position String,
    point_position_name String 
)   
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ham.dim_qycs_component_local', '{replica}')
ORDER BY component_id
SETTINGS index_granularity = 8192, storage_policy = 'multiple_disk'; 


CREATE TABLE ham.dim_qycs_component_all on cluster cluster_gio_with_shard
as ham.dim_qycs_component_local
ENGINE = Distributed('cluster_gio_with_shard', 'ham', 'dim_qycs_component_local', rand());




drop table event_hi_dcslog_51006_all on cluster cluster_gio_with_shard;