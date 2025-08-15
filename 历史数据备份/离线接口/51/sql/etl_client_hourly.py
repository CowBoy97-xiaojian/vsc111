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

HH = '{{ execution_date.add(hours=8).strftime("%H") }}'  # hive 分区字段hour

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d_%H") }}'

hfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d%H") }}'

be_cmd = f"beeline -i /home/udbac/.hiverc -u {HS2} -n udbac --hivevar DT={DT} --hivevar HH={HH} -f /home/udbac/hqls/"

be_cmd2 = f"beeline -i /home/udbac/.hiverc -u {HS2} -n udbac --hivevar DT={DT} --hivevar HH={hfmt} -f /home/udbac/hqls/"

ffmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

old_dt = '{{ (execution_date - macros.timedelta(days=1)).add(hours=8).strftime("%Y%m%d") }}'

dir_cmd = f'rm -rf /data/output/af_dcslog_hourly/{dfmt} && mkdir -p /data/output/af_dcslog_hourly/{dfmt} && cd /data/output/af_dcslog_hourly/{dfmt}'

becsv_cmd = f"""/home/udbac/beeline/CDH-5.16.2-1.cdh5.16.2.p0.8/bin/beeline --nullemptystring=true --showHeader=false --outputformat=csv2 \
                   --silent=true -u {HS2} -n udbac --hivevar DT={DT}  --hivevar HH={HH} -f /home/udbac/hqls/"""

lftp_cmd = "lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3964 sftp://10.252.180.2/incoming/cmccsales_270cm/"

default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': failure_callback
}

with DAG('etl_client_hourly',
         default_args=default_args,
         catchup=False,
        # concurrency=8,
        # max_active_runs=7,
         schedule_interval='15 * * * *',
         ) as dag:

    etl_ods_client_raw = BashOperator(
        task_id="etl_ods_client_raw",
        bash_command=be_cmd + 'jituan/etl_ods_client_raw.hql',
        dag=dag,
    )

    etl_ods_client_delta = BashOperator(
        task_id='etl_ods_client_delta',
        bash_command=be_cmd + 'jituan/etl_ods_client_delta.hql',
        on_failure_callback=failure_callback,
        dag=dag,
    )

    etl_dwd_client_event_di = BashOperator(
        task_id='etl_dwd_client_event_di',
        bash_command=be_cmd + 'jituan/etl_dwd_client_event_di.hql',
        on_failure_callback=failure_callback,
        dag=dag,
    )


    check_dcslogs_jituan = BashOperator(
        task_id="check_dcslogs_jituan",
        bash_command=f"""
        sh -x /home/udbac/bin/check_hdfs.sh {DT} {HH} dcslogs_jituan
        """,
        dag=dag,
    )


    #正常流程
    check_dcslogs_jituan >> etl_ods_client_raw >> etl_ods_client_delta >> etl_dwd_client_event_di



