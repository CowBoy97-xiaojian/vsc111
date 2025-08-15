from airflow.operators.dummy import DummyOperator
from pendulum import timezone
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import ‘
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

with DAG('rpt_51010_hourly',
         default_args=default_args,
         catchup=False,
         schedule_interval='15 * * * *'
         ) as dag:

    sensor_etl_dcslog_hourly = ExternalTaskSensor(
        task_id="sensor_etl_dcslog_hourly",
        external_dag_id='etl_dcslog_hourly',
        external_task_id='etl_dwd_dcslog_event_di',
        timeout=18000,
        allowed_states=['success'],
        mode="reschedule")

    rpt_hachi_JZYY_XTB1_51010_hourly = BashOperator(
        task_id="rpt_hachi_JZYY_XTB1_51010_hourly",
        bash_command=f"""
        {bedsv_cmd}/h5/etl_ads_rpt_hachi_51010_hourly.hql
        """,
        dag=dag,
        max_active_tis_per_dag=6
    )

    export_51010_hourly = BashOperator(
        task_id="export_51010_hourly",
        bash_command=f"""
        cd /home/udbac/output/51010_hourly/
        sh /home/udbac/bin/hachi_bigtable_10_hourly.sh {dfmt} JZYY_XTB1_51010 {DT} {HH}
        export_sum=$(cat /home/udbac/output/51010_hourly/{dfmt}/{dfmt}_{HH}/export_{dfmt}_JZYY_XTB1_51010_{HH}.log)
        hive_sum=$(cat /home/udbac/output/51010_hourly/{dfmt}/{dfmt}_{HH}/hive_{dfmt}_JZYY_XTB1_51010_{HH}.log)
        export_dev=$((export_sum-hive_sum))
        echo "export_sum = $export_sum , hive_sum = $hive_sum, export_dev = $export_dev"
        if [ "$export_dev" -gt -50 -a "$export_dev" -lt 50 -a "$export_sum" -ne 0 ];then
            echo "{HH}小时:hive库的数据量和接口数据文件的数据量一致"
        else
            sh /data/bin/not_exists.sh
        fi
        #if [ "$export_sum" != "$hive_sum" ]
        #then
        #  sh /data/bin/not_exists.sh
        #else
        #  echo "{HH}小时:hive库的数据量和接口数据文件的数据量一致" 
        #fi
        """,
        dag=dag,
        max_active_tis_per_dag=8
    )


    sensor_etl_dcslog_gio_hourly = ExternalTaskSensor(
        task_id="sensor_etl_dcslog_gio_hourly",
        external_dag_id='seatunnel_etl_client_dcslog_gio_hourly',
        external_task_id='etl_dwd_dcslog_event_di_gio',
        timeout=10800,
        allowed_states=['success'],
        mode="reschedule")

    transform_name_daily = BashOperator(
        task_id="transform_name_daily",
        bash_command=f"""
        if [ '23' -eq {HH} ]
        then
           cd /home/udbac/output/51010_hourly/{dfmt}
           flag_sum=$(ls -l -R /home/udbac/output/51010_hourly/{dfmt} | grep flag | wc -l)
           if [ '24' -eq $flag_sum ]
           then
             sh /home/udbac/bin/transform_name_cp_51010.sh {dfmt} {DT}
           else
             #等待30分钟，再进行重跑
             sleep 1800
             echo "小时数据未全部导出,以下是完成小时列表:"
             ls -l -R /home/udbac/output/51010_hourly/{dfmt} | grep flag
             sh /home/udbac/bin/not_exists.sh
           fi
        else
            echo '未到23点账期,不运行'  
        fi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_51010 = BashOperator(
        task_id="push_51010",
        bash_command=f"""
        cd /home/udbac/output/51010_hourly/{dfmt}/
        if [ '23' -eq {HH} ];then
           sh /home/udbac/bin/new_push_51010.sh {DT}
           ct=$(ls *.dat.gz | wc -l)
           sh /home/udbac/bin/write_count_51.sh 51010 {DT} $ct
        else
           echo '未到23点账期,不运行'
        fi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    sensor_etl_dcslog_hourly >> sensor_etl_dcslog_gio_hourly >> rpt_hachi_JZYY_XTB1_51010_hourly >> export_51010_hourly >> transform_name_daily >> push_51010
