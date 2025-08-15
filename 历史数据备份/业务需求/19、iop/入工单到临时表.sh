#!/bin/bash

DTF=$1
DT=$(date -d $DTF +"%Y-%m-%d")

#1、清空iop本地表
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="truncate table ham.rule_engine_local on cluster cluster_gio_with_shard"

#2、导入数据
clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="insert into ham.rule_engine_all select marketing_id,area_location,area_name from ham.dim_client_work_order_d_all where area_name='业务楼层区域' and  dt ='${DT}'"

        insert into ham.rule_engine_all select marketing_id,area_location,area_name from ham.dim_client_work_order_d_all where area_name='业务楼层区域' and dt = '2023-06-25'


         ck_rpt_iop/ck_get_iop.sh



    #iop_work入库
    ck_flink_iop = BashOperator(
        task_id="ck_flink_iop",
        bash_command=f"""
        sh -x /home/udbac/bin/ck_rpt_iop/ck_get_iop.sh {dfmt}
        """,
        dag=dag
    )