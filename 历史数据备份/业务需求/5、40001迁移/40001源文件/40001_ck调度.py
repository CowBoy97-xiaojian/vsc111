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
        dingding_conn_id='dingding',
        message_type='text',
        message=message,
        at_all=False,
        at_mobiles=['18951846665','17612930601','18338395282','15309476679','13921442403','18715511862','13814022856']
    ).execute(context)

ffmt = '{{ execution_date.add(hours=8).strftime("%Y-%m-%d") }}'
dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'


output = '/home/udbac/output_ck/40001_daily_ck'


default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}


with DAG('ck_rpt_qycs_40001',
         default_args=default_args,
         catchup=False,
         max_active_tasks=8,
         max_active_runs=7,
         schedule_interval='40 7 * * *'
         ) as dag:

    ck_rpt_qycs_40001 = BashOperator(
        task_id="ck_rpt_qycs_40001",
        bash_command=f"""
        #执行sql语句插入到表中
        sh /home/udbac/hqls_ck/h5/ck_etl_ads_rpt_hachi_40001.sh {dfmt}
        #将SQL表中的数据加载到本地
        sh /home/udbac/bin/ck_bin/ck_put_40001.sh
	    cd /home/udbac/output_ck/40001_daily_ck
        echo 'push success' 
        #mkdir /home/udbac/QY_51006/{dfmt}
        #cd /home/udbac/QY_51006/{dfmt}
        #cp /home/udbac/40001_out/* ./
        #chmod 755 /home/udbac/QY_51006/{dfmt}
        #chmod 644 /home/udbac/QY_51006/{dfmt}/*
        """,
        dag=dag,
	max_active_tis_per_dag=1
    )

