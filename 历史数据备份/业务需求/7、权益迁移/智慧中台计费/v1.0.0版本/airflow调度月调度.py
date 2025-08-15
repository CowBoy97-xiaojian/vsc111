from airflow.operators.dummy import DummyOperator
from pendulum import timezone
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.dingding.operators.dingding import DingdingOperator
from airflow.operators.python import PythonOperator

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

DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # hive 分区字段dt

HH = '{{ execution_date.add(hours=8).strftime("%H") }}'  # hive 分区字段hour


ffmt = '{{ execution_date.add(hours=8).strftime("%Y-%m") }}'

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m") }}'

be_cmd = f"beeline -i /home/udbac/.hiverc -u {HS2} -n udbac --hivevar DT={ffmt} --hivevar FT={dfmt} -f /home/udbac/hqls/"

lftp_cmd = 'lftp -u sftp_coc270,SFtp_cOc270! -p 3964 sftp://10.252.180.2/incoming/EC-DATA/'

default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('rpt_zhzt_jifei_month',
         default_args=default_args,
         catchup=False,
         max_active_tasks=8,
         max_active_runs=7,
         schedule_interval='20 7 27 * *'
         #schedule_interval='20 7 * * *'
         ) as dag:

    etl_ods_zhzt_jf_dm = BashOperator(
        task_id="etl_ods_zhzt_jf_dm",
        bash_command=be_cmd + 'jituan/etl_ods_zhzt_jf_dm.hql',
        dag=dag,
    )


    rpt_createcode_daily = BashOperator(
        task_id="rpt_createcode_daily",
        bash_command=f"""sh -x /home/udbac/bin/hachi_bigtable_zhzt_jsfei_month.sh {dfmt} {ffmt}""",
        dag=dag,
    )

    rpt_push = BashOperator(
        task_id="rpt_push",
        bash_command=f"""
        cd /home/udbac/output/zhzt_jifei/{dfmt}        
        {lftp_cmd}/029/ -e "mput -e *.txt.gz;mput -e *.chk; exit" 
        echo "push success"
        """,
        dag=dag,
    )

    etl_ods_zhzt_jf_dm >> rpt_createcode_daily >> rpt_push
