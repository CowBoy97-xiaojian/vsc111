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


DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # hive 分区字段dt

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'
ffmt = '{{ execution_date.add(hours=8).strftime("%Y-%m-%d") }}'
outputpath = '/home/udbac/output_ck/51008_daily_ck'

default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('ck_rpt_51008_daily',
         default_args=default_args,
         catchup=False,
         schedule_interval='05 08 * * *'
         ) as dag:
    #获取远程的文件，确定下载到本地---只要没有就等待（循环等待）
    ck_get_ld_workorder_iop = BashOperator(
        task_id="ck_get_ld_workorder_iop",
        bash_command=f"""
        sh -x /home/udbac/bin/ck_rpt_51008/ck_get_ld_workorder.sh {dfmt}
        """,
        dag=dag
    )
   
    #维表入库
    ck_dim_client_ld_zdy_d_iop = BashOperator(
        task_id="ck_dim_client_ld_zdy_d_iop",
        bash_command=f"""
        sh -x /home/udbac/bin/ck_rpt_51008/ck_dim_client_work_order_d.sh {dfmt}
        """,
        dag=dag
    )
    ck_get_ld_workorder_iop >> ck_dim_client_ld_zdy_d_iop