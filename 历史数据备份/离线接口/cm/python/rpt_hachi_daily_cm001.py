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
    message = '???? AIRFLOW TASK FAILURE TIPS:\n' \
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

#becsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=csv2 \
#                   -u {HS2} -n udbac --hivevar DT={DT}  -f /opt/airflow"""

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

bedsv_cmd_guangdong = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=guangdong --hivevar PROV_ID=200  -f /home/udbac/hqls/"""

bedsv_cmd_zhejiang = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=zhejiang --hivevar PROV_ID=571  -f /home/udbac/hqls/"""

bedsv_cmd_hebei = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=hebei --hivevar PROV_ID=311  -f /home/udbac/hqls/"""

bedsv_cmd_henan = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=henan --hivevar PROV_ID=371  -f /home/udbac/hqls/"""

bedsv_cmd_heilongjiang = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=heilongjiang --hivevar PROV_ID=451  -f /home/udbac/hqls/"""

bedsv_cmd_shandong = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=shandong --hivevar PROV_ID=531  -f /home/udbac/hqls/"""

bedsv_cmd_guangxi = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=guangxi --hivevar PROV_ID=771  -f /home/udbac/hqls/"""


default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('rpt_hachi_daily_cm001',
         default_args=default_args,
         catchup=False,
         max_active_tasks=8,
         max_active_runs=7,
         schedule_interval='56 1 * * *'
         ) as dag:

    etl_shanghai = BashOperator(
        task_id="etl_shanghai",
        bash_command=f"""
        {bedsv_cmd_shanghai}/jituan/etl_ads_hachi_cm001_shanghai.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_shanghai_gio = BashOperator(
        task_id="etl_shanghai_gio",
        bash_command=f"""
        {bedsv_cmd_shanghai}/jituan/etl_ads_hachi_cm001_shanghai_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_shanghai = BashOperator(
        task_id="export_shanghai",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001.sh {dfmt} CM001 210 shanghai
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_shanghai = BashOperator(
        task_id="push_shanghai",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/shanghai
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} shanghai
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guangdong = BashOperator(
        task_id="etl_guangdong",
        bash_command=f"""
        {bedsv_cmd_guangdong}/jituan/etl_ads_hachi_cm001_shanghai.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guangdong_gio = BashOperator(
        task_id="etl_guangdong_gio",
        bash_command=f"""
        {bedsv_cmd_guangdong}/jituan/etl_ads_hachi_cm001_shanghai_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_guangdong = BashOperator(
        task_id="export_guangdong",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001.sh {dfmt} CM001 200 guangdong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_guangdong = BashOperator(
        task_id="push_guangdong",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/guangdong
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} guangdong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_zhejiang = BashOperator(
        task_id="etl_zhejiang",
        bash_command=f"""
        {bedsv_cmd_zhejiang}/jituan/etl_ads_hachi_cm001_shanghai.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_zhejiang_gio = BashOperator(
        task_id="etl_zhejiang_gio",
        bash_command=f"""
        {bedsv_cmd_zhejiang}/jituan/etl_ads_hachi_cm001_shanghai_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_zhejiang = BashOperator(
        task_id="export_zhejiang",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001.sh {dfmt} CM001 571 zhejiang
#        sh  /home/udbac/bin/hachi_bigtable_prov_cm001_test.sh {dfmt} CM001 571 zhejiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_zhejiang = BashOperator(
        task_id="push_zhejiang",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/zhejiang
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} zhejiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )


    etl_hebei = BashOperator(
        task_id="etl_hebei",
        bash_command=f"""
        {bedsv_cmd_hebei}/jituan/etl_ads_hachi_cm001_shanghai.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_hebei_gio = BashOperator(
        task_id="etl_hebei_gio",
        bash_command=f"""
        {bedsv_cmd_hebei}/jituan/etl_ads_hachi_cm001_shanghai_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_hebei = BashOperator(
        task_id="export_hebei",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001.sh {dfmt} CM001 311 hebei
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_hebei = BashOperator(
        task_id="push_hebei",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/hebei
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} hebei
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_henan = BashOperator(
        task_id="etl_henan",
        bash_command=f"""
        {bedsv_cmd_henan}/jituan/etl_ads_hachi_cm001_shanghai.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_henan_gio = BashOperator(
        task_id="etl_henan_gio",
        bash_command=f"""
        {bedsv_cmd_henan}/jituan/etl_ads_hachi_cm001_shanghai_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_henan = BashOperator(
        task_id="export_henan",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001.sh {dfmt} CM001 371 henan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_henan = BashOperator(
        task_id="push_henan",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/henan
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} henan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_jiangsu = BashOperator(
        task_id="etl_jiangsu",
        bash_command=f"""
        {bedsv_cmd_jiangsu}/jituan/etl_ads_hachi_cm001_shanghai.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_jiangsu_gio = BashOperator(
        task_id="etl_jiangsu_gio",
        bash_command=f"""
        {bedsv_cmd_jiangsu}/jituan/etl_ads_hachi_cm001_shanghai_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_jiangsu = BashOperator(
        task_id="export_jiangsu",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001.sh {dfmt} CM001 250 jiangsu
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_jiangsu = BashOperator(
        task_id="push_jiangsu",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/jiangsu
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} jiangsu
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )


    etl_shandong = BashOperator(
        task_id="etl_shandong",
        bash_command=f"""
        {bedsv_cmd_shandong}/jituan/etl_ads_hachi_cm001_shanghai.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_shandong_gio = BashOperator(
        task_id="etl_shandong_gio",
        bash_command=f"""
        {bedsv_cmd_shandong}/jituan/etl_ads_hachi_cm001_shanghai_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_shandong = BashOperator(
        task_id="export_shandong",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001.sh {dfmt} CM001 531 shandong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_shandong = BashOperator(
        task_id="push_shandong",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/shandong
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} shandong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )


    etl_guangxi = BashOperator(
        task_id="etl_guangxi",
        bash_command=f"""
        {bedsv_cmd_guangxi}/jituan/etl_ads_hachi_cm001_shanghai.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guangxi_gio = BashOperator(
        task_id="etl_guangxi_gio",
        bash_command=f"""
        {bedsv_cmd_guangxi}/jituan/etl_ads_hachi_cm001_shanghai_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_guangxi = BashOperator(
        task_id="export_guangxi",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001.sh {dfmt} CM001 771 guangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_guangxi = BashOperator(
        task_id="push_guangxi",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/guangxi
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} guangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_chongqing = BashOperator(
        task_id="etl_chongqing",
        bash_command=f"""
        {bedsv_cmd_chongqing}/jituan/etl_ads_hachi_cm001_chongqing.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_chongqing_gio = BashOperator(
        task_id="etl_chongqing_gio",
        bash_command=f"""
        {bedsv_cmd_chongqing}/jituan/etl_ads_hachi_cm001_chongqing_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_chongqing = BashOperator(
        task_id="export_chongqing",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001_chongqing.sh {dfmt} CM001 230 chongqing
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_chongqing = BashOperator(
        task_id="push_chongqing",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/chongqing
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} chongqing
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_heilongjiang = BashOperator(
        task_id="etl_heilongjiang",
        bash_command=f"""
        {bedsv_cmd_heilongjiang}/jituan/etl_ads_hachi_cm001_shanghai.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_heilongjiang_gio = BashOperator(
        task_id="etl_heilongjiang_gio",
        bash_command=f"""
        {bedsv_cmd_heilongjiang}/jituan/etl_ads_hachi_cm001_shanghai_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_heilongjiang = BashOperator(
        task_id="export_heilongjiang",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001.sh {dfmt} CM001 451 heilongjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_heilongjiang = BashOperator(
        task_id="push_heilongjiang",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/heilongjiang
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} heilongjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_xinjiang = BashOperator(
        task_id="etl_xinjiang",
        bash_command=f"""
        {bedsv_cmd_xinjiang}/jituan/etl_ads_hachi_cm001_xinjiang.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_xinjiang_gio = BashOperator(
        task_id="etl_xinjiang_gio",
        bash_command=f"""
        {bedsv_cmd_xinjiang}/jituan/etl_ads_hachi_cm001_xinjiang_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_xinjiang = BashOperator(
        task_id="export_xinjiang",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001_xinjiang.sh {dfmt} CM001 991 xinjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_xinjiang = BashOperator(
        task_id="push_xinjiang",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/xinjiang
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} xinjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )    

    etl_yunnan = BashOperator(
        task_id="etl_yunnan",
        bash_command=f"""
        {bedsv_cmd_yunnan}/jituan/etl_ads_hachi_cm001_yunnan.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_yunnan_gio = BashOperator(
        task_id="etl_yunnan_gio",
        bash_command=f"""
        {bedsv_cmd_yunnan}/jituan/etl_ads_hachi_cm001_yunnan_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_yunnan = BashOperator(
        task_id="export_yunnan",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm001_yunnan.sh {dfmt} CM001 871 yunnan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_yunnan = BashOperator(
        task_id="push_yunnan",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/new_CM001/yunnan
        sh /home/udbac/bin/new_push_cm.sh CM001 {DT} yunnan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_shanghai >>  etl_shanghai_gio >> export_shanghai >> push_shanghai
    etl_guangdong >> etl_guangdong_gio >> export_guangdong >> push_guangdong
    etl_zhejiang >> etl_zhejiang_gio >> export_zhejiang >> push_zhejiang
    etl_hebei >> etl_hebei_gio >> export_hebei >> push_hebei
    etl_henan >> etl_henan_gio >> export_henan >> push_henan
    etl_jiangsu >> etl_jiangsu_gio >> export_jiangsu >> push_jiangsu
    etl_shandong >> etl_shandong_gio >> export_shandong >> push_shandong
    etl_guangxi >> etl_guangxi_gio >> export_guangxi >> push_guangxi
    etl_chongqing >> etl_chongqing_gio >> export_chongqing >> push_chongqing
    etl_heilongjiang >> etl_heilongjiang_gio >> export_heilongjiang >> push_heilongjiang
    etl_xinjiang >> etl_xinjiang_gio >> export_xinjiang >> push_xinjiang
    etl_yunnan >> etl_yunnan_gio >> export_yunnan >> push_yunnan

