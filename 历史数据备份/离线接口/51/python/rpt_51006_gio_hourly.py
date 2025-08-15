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

bedsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar HH={HH}  -f /home/udbac/hqls/"""

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'


default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('rpt_51006_gio_hourly',
         default_args=default_args,
         catchup=False,
         schedule_interval='15 * * * *'
         ) as dag:
    sensor_etl_dcslog_51006 = ExternalTaskSensor(
        task_id="sensor_etl_dcslog_51006",
        external_dag_id='etl_client_dcslog_gio_hourly',
    #    external_task_id='etl_dwd_dcslog_event_di_gio_51010',
        external_task_id='etl_dwd_dcslog_event_di_gio',
        timeout=10800,
        allowed_states=['success'],
        mode="reschedule")
    
    rpt_hachi_JZYY_XTB1_51006_gio_hourly = BashOperator(
        task_id="rpt_hachi_JZYY_XTB1_51006_gio_hourly",
        bash_command=f"""
        {bedsv_cmd}/h5/etl_ads_rpt_hachi_51006_gio_hourly.hql
        """,
        dag=dag,
        max_active_tis_per_dag=6
    )

    export_51006_gio_hourly = BashOperator(
        task_id="export_51006_gio_hourly",
        bash_command=f"""
        cd /home/udbac/output/51006_gio_hourly/
        sh /home/udbac/bin/hachi_bigtable_06_gio_hourly.sh {dfmt} JZYY_XTB1_51006_gio {DT} {HH}
        export_sum=$(cat /home/udbac/output/51006_gio_hourly/{dfmt}/{dfmt}_{HH}/export_{dfmt}_JZYY_XTB1_51006_gio_{HH}.log)
        hive_sum=$(cat /home/udbac/output/51006_gio_hourly/{dfmt}/{dfmt}_{HH}/hive_{dfmt}_JZYY_XTB1_51006_gio_{HH}.log)
#        if [ "$export_sum" != "$hive_sum" ]
#        then
#          sh /data/bin/not_exists.sh
#        else
#          echo "{HH}小时:hive库的数据量和接口数据文件的数据量一致" 
#        fi

        export_dev=$((export_sum-hive_sum))
        if [ "$export_dev" -gt -200 -a "$export_dev" -lt 200 -a "$export_sum" -ne 0 ];then
            echo "{HH}小时:hive库的数据量和接口数据文件的数据量一致"
        else
            sh /data/bin/not_exists.sh
        fi

        """,
        dag=dag,
        max_active_tis_per_dag=2
    )

    sensor_etl_dcslog_51006 >> rpt_hachi_JZYY_XTB1_51006_gio_hourly >> export_51006_gio_hourly
