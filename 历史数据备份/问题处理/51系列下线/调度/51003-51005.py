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

#becsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=csv2 \
#                   -u {HS2} -n udbac --hivevar DT={DT}  -f /opt/airflow"""

bedsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  -f /home/udbac/hqls/"""

becsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=csv2 \
                    --silent=true -u {HS2} -n udbac --hivevar DT={DT}  -f /home/udbac/hqls/"""

ffmt = '{{ execution_date.add(hours=8).strftime("%Y_%m_%d") }}'

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

dir_cmd = f'mkdir -p /home/udbac/output/af_daily/{dfmt} && cd /home/udbac/output/af_daily/{dfmt}'

#lftp_cmd_ha = "lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3964 sftp://10.252.180.2/incoming/cmccsales_270cm/"
lftp_cmd_ha = "lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3968 sftp://10.252.180.2/incoming/cmccsales_270cm/"

lftp_qycs = "lftp -u cm_sftp,hYuk161a0utREroimVvg -p 10022 sftp://10.253.182.69/home/udbac/QY_51006/"

default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('rpt_hachi_daily',
         default_args=default_args,
         catchup=False,
         max_active_tasks=8,
         max_active_runs=7,
         schedule_interval='56 0 * * *'
         ) as dag:
    sensor_etl_ads_rpt_ev_ch_tr_pr_di = ExternalTaskSensor(
        task_id="sensor_etl_ads_rpt_ev_ch_tr_pr_di",
        external_dag_id='etl_dcslog_daily',
        external_task_id='etl_ads_rpt_ev_ch_tr_pr_di',
        timeout=1800,
        allowed_states=['success'],
        mode="reschedule")

    rpt_hachi_JZYY_XTB1_51003 = BashOperator(
        task_id="rpt_hachi_JZYY_XTB1_51003",
        bash_command=f"""{dir_cmd}
        FNAME=a_10000_{dfmt}_JZYY_XTB1_51003_00_001.dat
        VNAME=a_10000_{dfmt}_JZYY_XTB1_51003_00_001.verf
        {becsv_cmd}/h5/rpt_hachi_JZYY_XTB1_51003.hql | sed '/\\r/d' | sed '/^$/d' | iconv -c -t GBK | sed "s/\\x2c/\\x80/g" > $FNAME
        echo "$FNAME,$(wc -c < $FNAME),$(wc -l < $FNAME),{dfmt},$(date -d @$(stat -c %Y $FNAME) +%Y%m%d%H%M%S)" \
            | iconv -t GBK | sed "s/\\x2c/\\x80/g" > $VNAME
        {lftp_cmd_ha} -e "mput -c $FNAME $VNAME; exit;"
        """,
        dag=dag,
    )

    rpt_hachi_JZYY_XTB1_51004 = BashOperator(
        task_id="rpt_hachi_JZYY_XTB1_51004",
        bash_command=f"""{dir_cmd}
        FNAME=a_10000_{dfmt}_JZYY_XTB1_51004_00_001.dat
        VNAME=a_10000_{dfmt}_JZYY_XTB1_51004_00_001.verf
        {becsv_cmd}/h5/rpt_hachi_JZYY_XTB1_51004.hql | sed '/\\r/d' | sed '/^$/d' | iconv -c -t GBK | sed "s/\\x2c/\\x80/g" > $FNAME
        echo "$FNAME,$(wc -c < $FNAME),$(wc -l < $FNAME),{dfmt},$(date -d @$(stat -c %Y $FNAME) +%Y%m%d%H%M%S)" \
            | iconv -t GBK | sed "s/\\x2c/\\x80/g" > $VNAME
        {lftp_cmd_ha} -e "mput -c $FNAME $VNAME; exit;"
        """,
        dag=dag,
    )

    rpt_hachi_JZYY_XTB1_51005 = BashOperator(
        task_id="rpt_hachi_JZYY_XTB1_51005",
        bash_command=f"""{dir_cmd}
        FNAME=a_10000_{dfmt}_JZYY_XTB1_51005_00_001.dat
        VNAME=a_10000_{dfmt}_JZYY_XTB1_51005_00_001.verf
        {becsv_cmd}/h5/rpt_hachi_JZYY_XTB1_51005.hql | sed '/\\r/d' | sed '/^$/d' | iconv -c -t GBK | sed "s/\\x2c/\\x80/g" > $FNAME
        echo "$FNAME,$(wc -c < $FNAME),$(wc -l < $FNAME),{dfmt},$(date -d @$(stat -c %Y $FNAME) +%Y%m%d%H%M%S)" \
            | iconv -t GBK | sed "s/\\x2c/\\x80/g" > $VNAME
        {lftp_cmd_ha} -e "mput -c $FNAME $VNAME; exit;"
        """,
        dag=dag,
    )

    rpt_hachi_JZYY_XTB1_51206 = BashOperator(
        task_id="rpt_hachi_JZYY_XTB1_51206",
        bash_command=f"""{dir_cmd}
        FNAME=a_10000_{dfmt}_JZYY_XTB1_51206_00_001.dat
        VNAME=a_10000_{dfmt}_JZYY_XTB1_51206_00_001.verf
        {becsv_cmd}/h5/rpt_hachi_JZYY_XTB1_51206.hql | sed '/\\r/d' | sed '/^$/d' | iconv -c -t GBK | sed "s/\\x2c/\\x80/g" > $FNAME
        echo "$FNAME,$(wc -c < $FNAME),$(wc -l < $FNAME),{dfmt},$(date -d @$(stat -c %Y $FNAME) +%Y%m%d%H%M%S)" \
            | iconv -t GBK | sed "s/\\x2c/\\x80/g" > $VNAME
        {lftp_cmd_ha} -e "mput -c $FNAME $VNAME; exit;"
        """,
        dag=dag,
    )