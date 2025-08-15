from airflow.operators.dummy import DummyOperator
from pendulum import timezone
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.dingding.operators.dingding import DingdingOperator
from airflow.models import Variable

def failure_callback(context):
    """
    The function that will be executed on failure.

    :param context: The context of the executed task.
    :type context: dict
    """
    message = 'clickhouse 小时入库调度失败 AIRFLOW TASK FAILURE TIPS:\n' \
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

DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # hive 分区字段dt

HH = '{{ execution_date.add(hours=8).strftime("%H") }}'  # hive 分区字段hour

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d_%H") }}'

hfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d%H") }}'

ckfmt = '{{ execution_date.add(hours=8).strftime("%Y-%m-%d_%H:%M:%S") }}'

ffmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

old_dt = '{{ (execution_date - macros.timedelta(days=1)).add(hours=8).strftime("%Y%m%d") }}'

dir_cmd = f'rm -rf /home/udbac/output_ck/af_dcslog_hourly/{dfmt} && mkdir -p /home/udbac/output_ck/af_dcslog_hourly/{dfmt} && cd /home/udbac/output_ck/af_dcslog_hourly/{dfmt}'

outputpath = '/home/udbac/output_ck/51010_daily/{ffmt}'

default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}


with DAG('ck_rpt_51010_get_event_all',
         default_args=default_args,
         catchup=False,
         max_active_tasks=8,
         max_active_runs=6,
         schedule_interval='00 2 * * *',
         ) as dag:

    ck_getevent_all_51010 = BashOperator(
        task_id="ck_getevent_all_51010",
        bash_command=f"""
        /home/udbac/bin/ck_bin/ck_getevent_all_51010.sh {ffmt}
        """  ,
        dag=dag,
    )
