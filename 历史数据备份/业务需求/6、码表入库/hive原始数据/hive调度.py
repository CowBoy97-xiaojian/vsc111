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


HS2 = 'jdbc:hive2://master01:10000/ham'

DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # hive 分区字段dt

becsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=csv2 \
                    --silent=true -u {HS2} -n udbac --hivevar DT={DT}  -f /home/udbac/hqls/h5/"""

bedsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    --silent=true -u {HS2} -n udbac --hivevar DT={DT}  -f /home/udbac/hqls/h5/"""

ffmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

dir_cmd = f'mkdir -p /home/udbac/output/af_daily/{dfmt} && cd /home/udbac/output/af_daily/{dfmt}'

#lftp_cmd = 'lftp -u report_chama,OQC2RkMSpsPPemFB -p 8016 sftp://117.132.186.14/upload'
#lftp_cmd = 'lftp -u report_chama,OQC2RkMSpsPPemFB -p 32299 sftp://10.253.35.137/upload'
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

with DAG('rpt_file_daily',
         default_args=default_args,
         catchup=False,
         concurrency=8,
         max_active_runs=7,
         schedule_interval='10 2 * * *'
         ) as dag:


    rpt_event_detail_d = BashOperator(
        task_id="rpt_event_detail_d",
        bash_command=f"""{dir_cmd}
        echo "日期,编号,业务名称,页面名称,事件名称,事件码,是否首页,渠道id,渠道名称,渠道类型,页面id,pv,uv" > 全渠道汇总表_{ffmt}.CP.csv
        {becsv_cmd}/rpt_ev
        yent_detail_d.hql | sed '/\\r/d' | sed '/^$/d' >> 全渠道汇总表_{ffmt}.CP.csv
        {lftp_cmd}/udbac_daily -e "put -e 全渠道汇总表_{ffmt}.CP.csv; exit" """,
        dag=dag,
    )

    rpt_event_detail_d2y = BashOperator(
        task_id="rpt_event_detail_d2y",
        bash_command=f"""{dir_cmd}
        echo "日期,页面id,渠道id,渠道名称,渠道类型,pv,uv,页面链接" > 基础码数据表_{dfmt}.CP.csv
        {becsv_cmd}/rpt_event_detail_d2y.hql | sed '/\\r/d' | sed '/^$/d' >> 基础码数据表_{dfmt}.CP.csv
        {lftp_cmd}/udbac_daily -e "put -e 基础码数据表_{dfmt}.CP.csv; exit" """,
        dag=dag,
    )

    rpt_haoka_00100_d = BashOperator(
        task_id="rpt_haoka_00100_d",
        bash_command=f"""{dir_cmd}
        {bedsv_cmd}/rpt_haoka_00100_d.hql | sed '/\\r/d' | sed '/^$/d' > 0001_00100_{dfmt}_001.txt""",
        dag=dag,
    )

    rpt_haoka_00101_d = BashOperator(
        task_id="rpt_haoka_00101_d",
        bash_command=f"""{dir_cmd}
        #删除空白行
        {bedsv_cmd}/rpt_haoka_00101_d.hql | sed '/\\r/d' | sed '/^$/d' > 0001_00101_{dfmt}_001.txt""",
        dag=dag,
    )

    rpt_haoka_00200_d = BashOperator(
        task_id="rpt_haoka_00200_d",
        bash_command=f"""{dir_cmd}
        {bedsv_cmd}/rpt_haoka_00200_d.hql | sed '/\\r/d' | sed '/^$/d' | \
        split -a 3 --additional-suffix=.txt -d -l 50000 --numeric-suffixes=1 - 0001_00200_{dfmt}_ """,
        dag=dag,
    )

    rpt_haoka_00300_d = BashOperator(
        task_id="rpt_haoka_00300_d",
        bash_command=f"""{dir_cmd}
        {bedsv_cmd}/rpt_haoka_00300_d.hql | sed '/\\r/d' | sed '/^$/d' | \
        split -a 3 --additional-suffix=.txt -d -l 50000 --numeric-suffixes=1 - 0001_00300_{dfmt}_ """,
        dag=dag,
    )

    rpt_haoka_00400_d = BashOperator(
        task_id="rpt_haoka_00400_d",
        bash_command=f"""{dir_cmd}
        {bedsv_cmd}/rpt_haoka_00400_d.hql | sed '/\\r/d' | sed '/^$/d' | \
        split -a 3 --additional-suffix=.txt -d -l 50000 --numeric-suffixes=1 - 0001_00400_{dfmt}_ """,
        dag=dag,
    )

    rpt_haoka_00500_d = BashOperator(
        task_id="rpt_haoka_00500_d",
        bash_command=f"""{dir_cmd}
        {bedsv_cmd}/rpt_haoka_00500_d.hql | sed '/\\r/d' | sed '/^$/d' | \
        split -a 3 --additional-suffix=.txt -d -l 50000 --numeric-suffixes=1 - 0001_00500_{dfmt}_ """,
        dag=dag,
    )

    join_haoka = DummyOperator(task_id='join_haoka', trigger_rule="none_failed", dag=dag)


    rpt_event_detail_d
    rpt_event_detail_d2y
    rpt_haoka_00100_d >> join_haoka
    rpt_haoka_00101_d >> join_haoka
    rpt_haoka_00200_d >> join_haoka 
    rpt_haoka_00300_d >> join_haoka
    rpt_haoka_00400_d >> join_haoka
    rpt_haoka_00500_d >> join_haoka
