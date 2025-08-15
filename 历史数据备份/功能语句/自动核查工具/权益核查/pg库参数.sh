CREATE TABLE "data_white2" (
  "WT_COOKIES" varchar(100) COLLATE "pg_catalog"."default",
  "BILL_NO" varchar(100) COLLATE "pg_catalog"."default",
  "DATA_TIME" timestamp(6),
  "WT_EVENT_ID" varchar(100) COLLATE "pg_catalog"."default",
  "WT_ET" varchar(20) COLLATE "pg_catalog"."default",
  "WT_CURRENT_URL" varchar(100) COLLATE "pg_catalog"."default",
  "WT_COMPONENT_ID" varchar(100) COLLATE "pg_catalog"."default",
  "WT_MODULE_NO" varchar(100) COLLATE "pg_catalog"."default",
  "WT_POINT_POSITION" varchar(100) COLLATE "pg_catalog"."default",
  "WT_ENVNAME" varchar(100) COLLATE "pg_catalog"."default",
  "WT_PREPARE1" varchar(100) COLLATE "pg_catalog"."default"
)
;

实时程序

消费
gio-basicflow-aba9de4ce446b2d2

1、过滤手机号   list集合,符合这个集合里面的手机号才能通过

2、取一下字段参考 51006,权益区域曝光
WT_COOKIES ,
BILL_NO ,
DATA_TIME ,
WT_EVENT_ID ,
WT_ET ,
WT_CURRENT_URL ,
WT_COMPONENT_ID ,
WT_MODULE_NO ,
WT_POINT_POSITION ,
WT_ENVNAME ,
WT_PREPARE1

sink到pg


手机号白名单
CREATE TABLE "data_whitelist1" (
  "id" int4 NOT NULL,
  "phone" varchar(20) COLLATE "pg_catalog"."default",
  "province_id" int4,
  "del_flag" int4 DEFAULT 0,
  "create_user" int4,
  "create_time" timestamp(6),
  "update_user" int4,
  "update_time" timestamp(6),
 CONSTRAINT "data_whitelist_pkey" PRIMARY KEY ("id")
)
;

SELECT phone FROM "data_whitelist" where del_flag = 0;

insert into data_whitelist (id,phone,province_id,del_flag) values (1,'13948869613',1,0);
insert into data_whitelist (id,phone,province_id,del_flag) values (2,'13484769037',1,0);
insert into data_whitelist (id,phone,province_id,del_flag) values (3,'13848885851',1,0);
insert into data_whitelist (id,phone,province_id,del_flag) values (4,'13354689988',1,0);
insert into data_whitelist (id,phone,province_id,del_flag) values (5,'19564913926',1,0);


#埋点管理平台 添加物理机PG库访问权限
host    all             all             10.104.92.247/32        md5
host    all             all             10.104.92.242/32        md5
host    all             all             10.104.92.235/32        md5
host    all             all             10.104.92.229/32        md5
host    all             all             10.104.92.233/32        md5
host    all             all             10.104.92.228/32        md5
host    all             all             10.104.92.241/32        md5
host    all             all             10.104.92.239/32        md5
host    all             all             10.104.92.245/32        md5
host    all             all             10.104.92.232/32        md5
host    all             all             10.104.92.234/32        md5
host    all             all             10.104.92.238/32        md5
host    all             all             10.104.92.240/32        md5
host    all             all             10.104.92.226/32        md5
host    all             all             10.104.92.236/32        md5

#埋点管理平台 添加物理机PG库访问权限
host    all             all             10.104.92.0/24        md5

10.104.92.0/24

192 128 64 32 16 8 4 2 1
1111111