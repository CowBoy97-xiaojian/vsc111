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

DT = '{{ execution_date.add(hours=8).to_date_string() }}'  


bedsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  -f /opt/airflow"""

becsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=csv2 \
                    --silent=true -u {HS2} -n udbac --hivevar DT={DT}  -f /home/udbac/hqls"""

ffmt = '{{ execution_date.add(hours=8).strftime("%Y_%m_%d") }}'

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'


lftp_cmd_ha = "lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3964 sftp://10.252.180.2/incoming/cmccsales_270cm/"

lftp_qycs = "lftp -u cm_sftp,hYuk161a0utREroimVvg -p 10022 sftp://10.253.182.69/home/udbac/QY_51006/"


bedsv_cmd_sichuan = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=sichuan --hivevar PROV_ID=280  -f /home/udbac/hqls/"""

bedsv_cmd_quanguo = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=quanguo --hivevar PROV_ID=000  -f /home/udbac/hqls/"""

bedsv_cmd_guizhou = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=guizhou --hivevar PROV_ID=851  -f /home/udbac/hqls/"""

bedsv_cmd_ningxia = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=ningxia --hivevar PROV_ID=951  -f /home/udbac/hqls/"""

bedsv_cmd_hubei = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=hubei --hivevar PROV_ID=270  -f /home/udbac/hqls/"""

bedsv_cmd_shanghai = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=shanghai --hivevar PROV_ID=210  -f /home/udbac/hqls/"""

bedsv_cmd_beijing = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=beijing --hivevar PROV_ID=100  -f /home/udbac/hqls/"""

bedsv_cmd_xinjiang = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=xinjiang --hivevar PROV_ID=991  -f /home/udbac/hqls/"""

bedsv_cmd_chongqing = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=chongqing --hivevar PROV_ID=230  -f /home/udbac/hqls/"""

bedsv_cmd_anhui = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=anhui --hivevar PROV_ID=551  -f /home/udbac/hqls/"""

bedsv_cmd_jiangsu = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=jiangsu --hivevar PROV_ID=250  -f /home/udbac/hqls/"""

bedsv_cmd_jiangxi = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=jiangxi --hivevar PROV_ID=791  -f /home/udbac/hqls/"""

bedsv_cmd_yunnan = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=yunnan --hivevar PROV_ID=871  -f /home/udbac/hqls/"""


default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('rpt_hachi_daily_prov',
         default_args=default_args,
         catchup=False,
         max_active_tasks=8,
         max_active_runs=7,
         schedule_interval='56 0 * * *'
         ) as dag:

    etl_jiangxi = BashOperator(
        task_id="etl_jiangxi",
        bash_command=f"""
        {bedsv_cmd_jiangxi}/jituan/etl_ads_hachi_cm001_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_jiangxi_gio = BashOperator(
        task_id="etl_jiangxi_gio",
        bash_command=f"""
        {bedsv_cmd_jiangxi}/jituan/etl_ads_hachi_cm001_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_jiangxi = BashOperator(
        task_id="export_jiangxi",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov.sh {dfmt} CM001 791 jiangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_jiangxi = BashOperator(
        task_id="push_jiangxi",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM001/jiangxi
        sh /home/udbac/bin/new_push_cm.sh CM_prov {DT} jiangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_sichuan = BashOperator(
        task_id="etl_sichuan",
        bash_command=f"""
        {bedsv_cmd_sichuan}/jituan/etl_ads_hachi_cm001_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_sichuan_gio = BashOperator(
        task_id="etl_sichuan_gio",
        bash_command=f"""
        {bedsv_cmd_sichuan}/jituan/etl_ads_hachi_cm001_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_sichuan = BashOperator(
        task_id="export_sichuan",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov.sh {dfmt} CM001 280 sichuan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_sichuan = BashOperator(
        task_id="push_sichuan",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM001/sichuan
        sh /home/udbac/bin/new_push_cm.sh CM_prov {DT} sichuan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_ningxia = BashOperator(
        task_id="etl_ningxia",
        bash_command=f"""
        {bedsv_cmd_ningxia}/jituan/etl_ads_hachi_cm001_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_ningxia_gio = BashOperator(
        task_id="etl_ningxia_gio",
        bash_command=f"""
        {bedsv_cmd_ningxia}/jituan/etl_ads_hachi_cm001_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_ningxia = BashOperator(
        task_id="export_ningxia",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov.sh {dfmt} CM001 951 ningxia
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_ningxia = BashOperator(
        task_id="push_ningxia",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM001/ningxia
        sh /home/udbac/bin/new_push_cm.sh CM_prov {DT} ningxia
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )


    etl_hubei = BashOperator(
        task_id="etl_hubei",
        bash_command=f"""
        {bedsv_cmd_hubei}/jituan/etl_ads_hachi_cm001_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_hubei_gio = BashOperator(
        task_id="etl_hubei_gio",
        bash_command=f"""
        {bedsv_cmd_hubei}/jituan/etl_ads_hachi_cm001_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_hubei = BashOperator(
        task_id="export_hubei",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov.sh {dfmt} CM001 270 hubei
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_hubei = BashOperator(
        task_id="push_hubei",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM001/hubei
        sh /home/udbac/bin/new_push_cm.sh CM_prov {DT} hebei
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )


    etl_beijing = BashOperator(
        task_id="etl_beijing",
        bash_command=f"""
        {bedsv_cmd_beijing}/jituan/etl_ads_hachi_cm001_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_beijing_gio = BashOperator(
        task_id="etl_beijing_gio",
        bash_command=f"""
        {bedsv_cmd_beijing}/jituan/etl_ads_hachi_cm001_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_beijing = BashOperator(
        task_id="export_beijing",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov.sh {dfmt} CM001 100 beijing
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_beijing = BashOperator(
        task_id="push_beijing",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM001/beijing
        sh /home/udbac/bin/new_push_cm.sh CM_prov {DT} beijing
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_quanguo = BashOperator(
        task_id="etl_quanguo",
        bash_command=f"""
        {bedsv_cmd_quanguo}/jituan/etl_ads_hachi_cm001_prov_000.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_quanguo_gio = BashOperator(
        task_id="etl_quanguo_gio",
        bash_command=f"""
        {bedsv_cmd_quanguo}/jituan/etl_ads_hachi_cm001_prov_000_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_quanguo = BashOperator(
        task_id="export_quanguo",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov.sh {dfmt} CM001 000 quanguo
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_quanguo = BashOperator(
        task_id="push_quanguo",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM001/quanguo/
        sh /home/udbac/bin/new_push_cm.sh CM_prov {DT} quanguo
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guizhou = BashOperator(
        task_id="etl_guizhou",
        bash_command=f"""
        {bedsv_cmd_guizhou}/jituan/etl_ads_hachi_cm001_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guizhou_gio = BashOperator(
        task_id="etl_guizhou_gio",
        bash_command=f"""
        {bedsv_cmd_guizhou}/jituan/etl_ads_hachi_cm001_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_guizhou = BashOperator(
        task_id="export_guizhou",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov.sh {dfmt} CM001 851 guizhou
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_guizhou = BashOperator(
        task_id="push_guizhou",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM001/guizhou
        sh /home/udbac/bin/new_push_cm.sh CM_prov {DT} guizhou
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )


    etl_anhui = BashOperator(
        task_id="etl_anhui",
        bash_command=f"""
        {bedsv_cmd_anhui}/jituan/etl_ads_hachi_cm001_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_anhui_gio = BashOperator(
        task_id="etl_anhui_gio",
        bash_command=f"""
        {bedsv_cmd_anhui}/jituan/etl_ads_hachi_cm001_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_anhui = BashOperator(
        task_id="export_anhui",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov.sh {dfmt} CM001 551 anhui
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_anhui = BashOperator(
        task_id="push_anhui",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM001/anhui
        sh /home/udbac/bin/new_push_cm.sh CM_prov {DT} anhui
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )


    etl_sichuan >> etl_sichuan_gio >> export_sichuan >> push_sichuan
    etl_quanguo >> etl_quanguo_gio >> export_quanguo >> push_quanguo
    etl_ningxia >> etl_ningxia_gio >> export_ningxia >> push_ningxia
    etl_hubei >>  etl_hubei_gio >> export_hubei >> push_hubei
    etl_beijing >> etl_beijing_gio >> export_beijing >> push_beijing
    etl_guizhou >>  etl_guizhou_gio >> export_guizhou >> push_guizhou
    etl_anhui >> etl_anhui_gio >> export_anhui >> push_anhui
    etl_jiangxi >> etl_jiangxi_gio >> export_jiangxi >> push_jiangxi

