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


HS2 = 'jdbc:hive2://master01:10000/ham'

DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # hive 分区字段dt

bedsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  -f /home/udbac/hqls"""

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

default_args = {
    'owner': 'udbac',
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('rpt_51010_daily',
         default_args=default_args,
         catchup=False,
         schedule_interval='56 0 * * *'
         ) as dag:

    sql_51010 = BashOperator(
        task_id="sql_51010",
        bash_command=f"""
        {bedsv_cmd}/h5/etl_ads_rpt_hachi_51010.hql
        """,
        dag=dag,
	max_active_tis_per_dag = 1
    )

    export = BashOperator(
        task_id="export",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_10.sh {dfmt}
        """,
        dag=dag,
        max_active_tis_per_dag = 1
    )

    push = BashOperator(
        task_id="push",
        bash_command=f"""
        cd /home/udbac/output/51010_daily/{dfmt}
#        sh  /home/udbac/bin/push_06-08.sh
#        ct=$(ls *.dat.gz | wc -l)
#        sh /home/udbac/bin/write_count_51.sh 51010 {DT} $ct
        """,
        dag=dag
    )

    sql_51010 >> export >> push
