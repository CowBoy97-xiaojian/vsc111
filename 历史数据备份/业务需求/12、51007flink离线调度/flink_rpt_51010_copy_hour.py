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



DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # 

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'
HH = '{{ execution_date.add(hours=8).strftime("%H") }}'


default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}



with DAG('flink_rpt_51010_copy_hour',
         default_args=default_args,
         catchup=False,
         schedule_interval='10 * * * *'
         ) as dag:


    copy_flink_51010 = BashOperator(
        task_id="copy_flink_51010",
        bash_command=f"""
        rm -rf /home/udbac/output_ck/51010_daily/{DT}/*_{HH}.dat.gz
        sh /home/udbac/output_ck/file_move.sh 51010 {DT}/{HH} {DT} {HH}
        """,
        dag=dag,
    )
