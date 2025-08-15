from airflow.operators.dummy_operator import DummyOperator
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

DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # hive 分区字段dt

ffmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

dir_cmd = f'mkdir -p /home/udbac/output_ck/af_daily/{dfmt} && cd /home/udbac/output/af_daily/{dfmt}'

default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('ck_rpt_file_daily',
         default_args=default_args,
         catchup=False,
         concurrency=8,
         max_active_runs=7,
         schedule_interval='10 2 * * *'
         ) as dag:
    
    ck_rpt_haoka_00100_d = BashOperator(
        task_id="ck_rpt_haoka_00100_d",
        bash_command=f"""{dir_cmd}
        sh /home/udbac/hqls_ck/h5/ck_rpt_mabiao_100.sh {DT} | sed '/\\r/d' | sed '/^$/d' > 0001_00100_{dfmt}_001.txt""",
        dag=dag,
    )

    ck_rpt_haoka_00101_d = BashOperator(
        task_id="ck_rpt_haoka_00101_d",
        bash_command=f"""{dir_cmd}
        sh /home/udbac/hqls_ck/h5/ck_rpt_mabiao_101.sh {DT} | sed '/\\r/d' | sed '/^$/d' > 0001_00101_{dfmt}_001.txt""",
        dag=dag,
    )

    ck_rpt_haoka_00200_d = BashOperator(
        task_id="ck_rpt_haoka_00200_d",
        bash_command=f"""{dir_cmd}
        sh /home/udbac/hqls_ck/h5/ck_rpt_mabiao_200.sh {DT} | sed '/\\r/d' | sed '/^$/d' | \
        split -a 3 --additional-suffix=.txt -d -l 50000 --numeric-suffixes=1 - 0001_00200_{dfmt}_ """,
        dag=dag,
    )

    ck_rpt_haoka_00300_d = BashOperator(
        task_id="ck_rpt_haoka_00300_d",
        bash_command=f"""{dir_cmd}
        sh /home/udbac/hqls_ck/h5/ck_rpt_mabiao_300.sh {DT} | sed '/\\r/d' | sed '/^$/d' | \
        split -a 3 --additional-suffix=.txt -d -l 50000 --numeric-suffixes=1 - 0001_00300_{dfmt}_ """,
        dag=dag,
    )

    ck_rpt_haoka_00400_d = BashOperator(
        task_id="ck_rpt_haoka_00400_d",
        bash_command=f"""{dir_cmd}
        sh /home/udbac/hqls_ck/h5/ck_rpt_mabiao_400.sh {DT} | sed '/\\r/d' | sed '/^$/d' | \
        split -a 3 --additional-suffix=.txt -d -l 50000 --numeric-suffixes=1 - 0001_00400_{dfmt}_ """,
        dag=dag,
    )

    ck_rpt_haoka_00500_d = BashOperator(
        task_id="ck_rpt_haoka_00500_d",
        bash_command=f"""{dir_cmd}
        sh /home/udbac/hqls_ck/h5/ck_rpt_mabiao_500.sh {DT} | sed '/\\r/d' | sed '/^$/d' | \
        split -a 3 --additional-suffix=.txt -d -l 50000 --numeric-suffixes=1 - 0001_00500_{dfmt}_ """,
        dag=dag,
    )

    join_haoka = DummyOperator(task_id='join_haoka', trigger_rule="none_failed", dag=dag)


