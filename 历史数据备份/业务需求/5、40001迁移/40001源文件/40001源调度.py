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


HS2 = 'jdbc:hive2://master01:10000/ham'

DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # hive 分区字段dt

#becsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=csv2 \
#                   -u {HS2} -n udbac --hivevar DT={DT}  -f /opt/airflow"""

bedsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  -f /opt/airflow"""

becsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=csv2 \
                    --silent=true -u {HS2} -n udbac --hivevar DT={DT}  -f /home/udbac/hqls/"""

ffmt = '{{ execution_date.add(hours=8).strftime("%Y_%m_%d") }}'

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

dir_cmd = f'mkdir -p /home/udbac/output/af_daily/{dfmt} && cd /home/udbac/output/af_daily/{dfmt}'

lftp_cmd_ha = "lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3964 sftp://10.252.180.2/incoming/cmccsales_270cm/"

lftp_qycs = "lftp -u cm_sftp,hYuk161a0utREroimVvg -p 10022 sftp://10.253.182.69/home/udbac/QY_51006/"

default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('rpt_qycs_40001',
         default_args=default_args,
         catchup=False,
         max_active_tasks=8,
         max_active_runs=7,
         schedule_interval='40 7 * * *'
         ) as dag:

    rpt_qycs_40001 = BashOperator(
        task_id="rpt_qycs_40001",
        bash_command=f"""{dir_cmd}
        {becsv_cmd}/h5/etl_ads_rpt_hachi_40001.hql
	sh -x /home/udbac/bin/40001.sh {dfmt}
	cd /home/udbac/40001_out 
        mkdir /home/udbac/QY_51006/{dfmt}
        cd /home/udbac/QY_51006/{dfmt}
        cp /home/udbac/40001_out/* ./
        chmod 755 /home/udbac/QY_51006/{dfmt}
        chmod 644 /home/udbac/QY_51006/{dfmt}/*
        """,
        dag=dag,
	max_active_tis_per_dag=1
    )