#cat rpt_cjhyx_di.py
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

becsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=csv2 \
                    --silent=true -u {HS2} -n udbac --hivevar DT={DT}  -f /home/udbac/hqls/h5/"""

bedsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    --silent=true -u {HS2} -n udbac --hivevar DT={DT}  -f /home/udbac/hqls/h5/"""

ffmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

dir_cmd = f'mkdir -p /home/udbac/output/af_daily/{dfmt} && cd /home/udbac/output/af_daily/{dfmt}'

lftp_cmd = 'lftp -u sftp_coc270,SFtp_cOc270! -p 3964 sftp://10.252.180.2/incoming'

md5awk_cmd = """awk '{print $2"|"$1}'"""

default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('rpt_cjhyx_di',
         default_args=default_args,
         catchup=False,
         max_active_tasks=8,
         max_active_runs=7,
         schedule_interval='40 00 * * *'
         ) as dag:


    rpt_cjhyx_di_thb = BashOperator(
        task_id="rpt_cjhyx_di_thb",
        bash_command=f"""{dir_cmd}
        {becsv_cmd}/cjhyx_thb.hql | sed '/\\r/d' | sed '/^$/d' > 场景化营销_特惠包_{ffmt}.csv """,
        dag=dag,
    )

    rpt_cjhyx_di_sxzq1 = BashOperator(
        task_id="rpt_cjhyx_di_sxzq1",
        bash_command=f"""{dir_cmd}
        {becsv_cmd}/cjhyx_sxzq1.hql | sed '/\\r/d' | sed '/^$/d' > 场景化营销_随心专区1_{ffmt}.csv """,
        dag=dag,
    )
   
    rpt_cjhyx_di_sxzq2 = BashOperator(
        task_id="rpt_cjhyx_di_sxzq2",
        bash_command=f"""{dir_cmd}
        {becsv_cmd}/cjhyx_sxzq2.hql | sed '/\\r/d' | sed '/^$/d' > 场景化营销_随心专区2_{ffmt}.csv """,
        dag=dag,
    )


    rpt_cjhyx_di_thb 
    rpt_cjhyx_di_sxzq1
    rpt_cjhyx_di_sxzq2

    /home/udbac/hqls/h5/cjhyx_thb.hql