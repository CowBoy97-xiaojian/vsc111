from airflow.operators.dummy import DummyOperator
from pendulum import timezone
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.dingding.operators.dingding import DingdingOperator
from airflow.models import Variable


def failure_callback(context):
    """
    The function that will be executed on failure.

    :param context: The context of the executed task.
    :type context: dict
    """
    message = '集运 AIRFLOW TASK FAILURE TIPS:\n' \
              'DAG:             {}\n' \
              'TASKS:           {}\n' \
              'EXECUTION_DATE:  {}\n' \
              'Reason:          {}\n' \
        .format(context['task_instance'].dag_id,
                context['task_instance'].task_id,
                context['execution_date'],
                context['exception'])
    return DingdingOperator(
        task_id='dingding_failure_callback',
        dingding_conn_id='dingding_default',
        message_type='text',
        message=message,
        at_all=False,
        at_mobiles=['18951846665','17612930601','18338395282','15309476679','13921442403','18715511862','13814022856']
    ).execute(context)

HS2 = 'jdbc:hive2://master01:10000/ham'

DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # 

HH = '{{ execution_date.add(hours=8).strftime("%H") }}'  # hive 分区字段hour

bedsv_cmd = f"""/home/udbac/beeline/CDH-5.16.2-1.cdh5.16.2.p0.8/bin/beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar HH={HH}  -f /data/hqls/"""

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'


default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('rpt_51007_hourly',
         default_args=default_args,
         catchup=False,
         schedule_interval='15 * * * *'
         ) as dag:

    sensor_etl_client_hourly = ExternalTaskSensor(
        task_id="sensor_etl_client_hourly",
        external_dag_id='etl_client_hourly',
        external_task_id='etl_dwd_client_event_di',
        timeout=3600,
        allowed_states=['success'],
        mode="reschedule")

    rpt_hachi_JZYY_XTB2_51007_hourly = BashOperator(
        task_id="rpt_hachi_JZYY_XTB2_51007_hourly",
        bash_command=f"""
        {bedsv_cmd}/jituan/etl_ads_hachi_jzyy_xtb2_51007_hourly.hql
        """,
        dag=dag,
        max_active_tis_per_dag=6
    )

    export_51007_hourly = BashOperator(
        task_id="export_51007_hourly",
        bash_command=f"""
        cd /data/output/51007_hourly/
        sh /data/bin/hachi_bigtable_07_hourly.sh {dfmt} JZYY_XTB2_51007 {DT} {HH}
        export_sum=$(cat /data/output/51007_hourly/{dfmt}/{dfmt}_{HH}/export_{dfmt}_JZYY_XTB2_51007_{HH}.log)
        hive_sum=$(cat /data/output/51007_hourly/{dfmt}/{dfmt}_{HH}/hive_{dfmt}_JZYY_XTB2_51007_{HH}.log)
        if [ "$export_sum" != "$hive_sum" ]
        then
          sh /data/bin/not_exists.sh
        else
          echo "{HH}小时:hive库的数据量和接口数据文件的数据量一致"
        fi
        """,
        dag=dag,
        max_active_tis_per_dag=2
    )

    transform_name_daily = BashOperator(
        task_id="transform_name_daily",
        bash_command=f"""
        if [ '23' -eq {HH} ]
        then
           cd /data/output/51007_hourly/{dfmt}
           flag_sum=$(ls -l -R /data/output/51007_hourly/{dfmt} | grep flag | wc -l)
           if [ '24' -eq $flag_sum ]
           then
             echo "开始汇总数据"
             sh /data/bin/transform_name_cp.sh {dfmt}
           else
             #等待30分钟，再进行重跑
             sleep 1800
             echo "小时数据未全部导出,以下是完成小时列表:"
             ls -l -R /data/output/51007_hourly/{dfmt} | grep flag
             sh /data/bin/not_exists.sh
           fi
        else
            echo '未到23点账期,不运行'  
        fi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_51007 = BashOperator(
        task_id="push_51007",
        bash_command=f"""
        cd /data/output/51007_hourly/{dfmt}/
        if [ '23' -eq {HH} ];then
           sh /data/bin/push_06-08.sh
           ct=$(ls *.dat.gz | wc -l)
           sh /data/bin/write_count_51.sh 51007 {DT} $ct
        else
           echo '未到23点账期,不运行'
        fi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    sensor_etl_client_hourly >> rpt_hachi_JZYY_XTB2_51007_hourly >> export_51007_hourly >> transform_name_daily  >> push_51007