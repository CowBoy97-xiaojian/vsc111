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
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar DT={DT} --hivevar HH={HH}  -f /home/udbac/hqls/"""

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'


default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}


with DAG('rpt_hachi_domain_daily',
         default_args=default_args,
         catchup=False,
         schedule_interval='15 00 * * *'
         ) as dag:

    rpt_hachi_domain_insert = BashOperator(
            task_id="rpt_hachi_domain_insert",
            bash_command=f"""
            {bedsv_cmd}/h5/etl_dim_rpt_domain.hql
            """,
            dag=dag,
            max_active_tis_per_dag=6
        )

    ck_rpt_hachi_domain_insert = BashOperator(
            task_id="ck_rpt_hachi_domain_insert",
            bash_command=f"""
            sh /home/udbac/hqls_ck/h5/ck_etl_dim_rpt_domain.sh {DT}
            """,
            dag=dag,
            max_active_tis_per_dag=6
        )
    
    rpt_hachi_domain_insert >> ck_rpt_hachi_domain_insert