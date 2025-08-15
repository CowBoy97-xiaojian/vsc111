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
dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'

bedsv_cmd_quanguo = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  --hivevar PROV=quanguo --hivevar DCSID=  --hivevar TRMNL_STYLE=  -f /home/udbac/hqls/"""

bedsv_cmd_liaoning = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  --hivevar PROV=liaoning --hivevar DCSID=9w6z  --hivevar TRMNL_STYLE=a10b1f67c7d288c1  -f /home/udbac/hqls/"""

bedsv_cmd_xinjiang = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  --hivevar PROV=xinjiang --hivevar DCSID=1g3e  --hivevar TRMNL_STYLE=a8c29f7abfd639b4  -f /home/udbac/hqls/"""

bedsv_cmd_guizhou = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  --hivevar PROV=guizhou --hivevar DCSID=9m2x  --hivevar TRMNL_STYLE=afdbd0f6b9a7ba30  -f /home/udbac/hqls/"""

bedsv_cmd_beijing = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  --hivevar PROV=beijing --hivevar DCSID=3s3i  --hivevar TRMNL_STYLE=a7464be8b200fe24  -f /home/udbac/hqls/"""

bedsv_cmd_jiangsu = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  --hivevar PROV=jiangsu --hivevar DCSID=8f4q  --hivevar TRMNL_STYLE=ae2308d5a08258b6  -f /home/udbac/hqls/"""

bedsv_cmd_sichuan = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  --hivevar PROV=sichuan --hivevar DCSID=6g7b  --hivevar TRMNL_STYLE=a6ba03d6884ee1c1  -f /home/udbac/hqls/"""

bedsv_cmd_jiangxi = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  --hivevar PROV=jiangxi --hivevar DCSID=3d2m  --hivevar TRMNL_STYLE=9a238b92b6612604  -f /home/udbac/hqls/"""

bedsv_cmd_yunnan = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  --hivevar PROV=yunnan --hivevar DCSID=8o8q  --hivevar TRMNL_STYLE=8273883304ad70fb  -f /home/udbac/hqls/"""

bedsv_cmd_guangdong = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=guangdong --hivevar DCSID=7p1j  --hivevar TRMNL_STYLE=b9e74b9d5da046ce  -f /home/udbac/hqls/"""

bedsv_cmd_zhejiang = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=zhejiang --hivevar DCSID=0o1j  --hivevar TRMNL_STYLE=8c19b47f237e28d3  -f /home/udbac/hqls/"""

bedsv_cmd_ningxia = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=ningxia --hivevar DCSID=9y7c  --hivevar TRMNL_STYLE=9b50668a016469ec  -f /home/udbac/hqls/"""

bedsv_cmd_hebei = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=hebei --hivevar DCSID=8d2o  --hivevar TRMNL_STYLE=a441e7427a44d390  -f /home/udbac/hqls/"""

bedsv_cmd_henan = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=henan --hivevar DCSID=5d3k  --hivevar TRMNL_STYLE=ad2e89b1010aed1e  -f /home/udbac/hqls/"""

bedsv_cmd_heilongjiang = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=heilongjiang --hivevar DCSID=4w6p  --hivevar TRMNL_STYLE=81ebc780373ee7cd  -f /home/udbac/hqls/"""

bedsv_cmd_shandong = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=shandong --hivevar DCSID=3j1d  --hivevar TRMNL_STYLE=abd580f08cf381e0  -f /home/udbac/hqls/"""

bedsv_cmd_guangxi = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=guangxi --hivevar DCSID=3r8r  --hivevar TRMNL_STYLE=b0839305cf5d280d  -f /home/udbac/hqls/"""

bedsv_cmd_anhui = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=anhui --hivevar DCSID=9o0p  --hivevar TRMNL_STYLE=ba81a7647a8bed5d  -f /home/udbac/hqls/"""

default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('rpt_hachi_daily_cm003',
         default_args=default_args,
         catchup=False,
         max_active_tasks=8,
         max_active_runs=7,
         schedule_interval='56 1 * * *'
         ) as dag:

    etl_yunnan = BashOperator(
        task_id="etl_yunnan",
        bash_command=f"""
        {bedsv_cmd_yunnan}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_yunnan_gio = BashOperator(
        task_id="etl_yunnan_gio",
        bash_command=f"""
        {bedsv_cmd_yunnan}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_yunnan = BashOperator(
        task_id="export_yunnan",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 871 yunnan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_yunnan = BashOperator(
        task_id="push_yunnan",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/yunnan
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} yunnan
        """,
        dag=dag,#        max_active_tis_per_dag=1
        max_active_tis_per_dag=1
    )

    etl_jiangxi = BashOperator(
        task_id="etl_jiangxi",
        bash_command=f"""
        {bedsv_cmd_jiangxi}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_jiangxi_gio = BashOperator(
        task_id="etl_jiangxi_gio",
        bash_command=f"""
        {bedsv_cmd_jiangxi}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_jiangxi = BashOperator(
        task_id="export_jiangxi",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 791 jiangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_jiangxi = BashOperator(
        task_id="push_jiangxi",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/jiangxi
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} jiangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_sichuan = BashOperator(
        task_id="etl_sichuan",
        bash_command=f"""
        {bedsv_cmd_sichuan}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_sichuan_gio = BashOperator(
        task_id="etl_sichuan_gio",
        bash_command=f"""
        {bedsv_cmd_sichuan}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_sichuan = BashOperator(
        task_id="export_sichuan",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 280 sichuan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_sichuan = BashOperator(
        task_id="push_sichuan",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/sichuan
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} sichuan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_jiangsu = BashOperator(
        task_id="etl_jiangsu",
        bash_command=f"""
        {bedsv_cmd_jiangsu}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_jiangsu_gio = BashOperator(
        task_id="etl_jiangsu_gio",
        bash_command=f"""
        {bedsv_cmd_jiangsu}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_jiangsu = BashOperator(
        task_id="export_jiangsu",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 250 jiangsu
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_jiangsu = BashOperator(
        task_id="push_jiangsu",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/jiangsu
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} jiangsu
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_beijing = BashOperator(
        task_id="etl_beijing",
        bash_command=f"""
        {bedsv_cmd_beijing}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_beijing_gio = BashOperator(
        task_id="etl_beijing_gio",
        bash_command=f"""
        {bedsv_cmd_beijing}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_beijing = BashOperator(
        task_id="export_beijing",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 100 beijing
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_beijing = BashOperator(
        task_id="push_beijing",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/beijing
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} beijing
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_liaoning = BashOperator(
        task_id="etl_liaoning",
        bash_command=f"""
        {bedsv_cmd_liaoning}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_liaoning_gio = BashOperator(
        task_id="etl_liaoning_gio",
        bash_command=f"""
        {bedsv_cmd_liaoning}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_liaoning = BashOperator(
        task_id="export_liaoning",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 240 liaoning
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_liaoning = BashOperator(
        task_id="push_liaoning",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/liaoning
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} liaoning
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_quanguo = BashOperator(
        task_id="etl_quanguo",
        bash_command=f"""
#        {bedsv_cmd_quanguo}/h5/etl_ads_hachi_cm003_000.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_quanguo = BashOperator(
        task_id="export_quanguo",
        bash_command=f"""
#        sh  /home/udbac/bin/hachi_bigtable_cm003_prov.sh {dfmt} CM003 000 quanguo
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_quanguo = BashOperator(
        task_id="push_quanguo",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/quanguo/
#        lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3964 sftp://10.252.180.2/incoming/cmpt/ -e "mput -c *.gz; mput -c *.verf; exit;"
        echo "push success"
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )


    etl_xinjiang = BashOperator(
        task_id="etl_xinjiang",
        bash_command=f"""
        {bedsv_cmd_xinjiang}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_xinjiang_gio = BashOperator(
        task_id="etl_xinjiang_gio",
        bash_command=f"""
        {bedsv_cmd_xinjiang}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_xinjiang = BashOperator(
        task_id="export_xinjiang",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 991 xinjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_xinjiang = BashOperator(
        task_id="push_xinjiang",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/xinjiang/
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} xinjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guizhou = BashOperator(
        task_id="etl_guizhou",
        bash_command=f"""
        {bedsv_cmd_guizhou}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guizhou_gio = BashOperator(
        task_id="etl_guizhou_gio",
        bash_command=f"""
        {bedsv_cmd_guizhou}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_guizhou = BashOperator(
        task_id="export_guizhou",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 851 guizhou
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_guizhou = BashOperator(
        task_id="push_guizhou",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/guizhou/
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} guizhou
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guangdong = BashOperator(
        task_id="etl_guangdong",
        bash_command=f"""
        {bedsv_cmd_guangdong}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guangdong_gio = BashOperator(
        task_id="etl_guangdong_gio",
        bash_command=f"""
        {bedsv_cmd_guangdong}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_guangdong = BashOperator(
        task_id="export_guangdong",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 200 guangdong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_guangdong = BashOperator(
        task_id="push_guangdong",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/guangdong/
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} guangdong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_zhejiang = BashOperator(
        task_id="etl_zhejiang",
        bash_command=f"""
        {bedsv_cmd_zhejiang}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_zhejiang_gio = BashOperator(
        task_id="etl_zhejiang_gio",
        bash_command=f"""
        {bedsv_cmd_zhejiang}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_zhejiang = BashOperator(
        task_id="export_zhejiang",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 571 zhejiang
#        sh  /home/udbac/bin/hachi_bigtable_prov_cm003_test.sh {dfmt} CM003 571 zhejiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_zhejiang = BashOperator(
        task_id="push_zhejiang",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/zhejiang/
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} zhejiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_ningxia = BashOperator(
        task_id="etl_ningxia",
        bash_command=f"""
        {bedsv_cmd_ningxia}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_ningxia_gio = BashOperator(
        task_id="etl_ningxia_gio",
        bash_command=f"""
        {bedsv_cmd_ningxia}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_ningxia = BashOperator(
        task_id="export_ningxia",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 951 ningxia
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_ningxia = BashOperator(
        task_id="push_ningxia",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/ningxia/
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} ningxia
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_hebei = BashOperator(
        task_id="etl_hebei",
        bash_command=f"""
        {bedsv_cmd_hebei}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_hebei_gio = BashOperator(
        task_id="etl_hebei_gio",
        bash_command=f"""
        {bedsv_cmd_hebei}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_hebei = BashOperator(
        task_id="export_hebei",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 311 hebei
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_hebei = BashOperator(
        task_id="push_hebei",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/hebei/
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} hebei
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_henan = BashOperator(
        task_id="etl_henan",
        bash_command=f"""
        {bedsv_cmd_henan}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_henan_gio = BashOperator(
        task_id="etl_henan_gio",
        bash_command=f"""
        {bedsv_cmd_henan}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_henan = BashOperator(
        task_id="export_henan",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 371 henan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_henan = BashOperator(
        task_id="push_henan",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/henan/
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} henan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_heilongjiang = BashOperator(
        task_id="etl_heilongjiang",
        bash_command=f"""
        {bedsv_cmd_heilongjiang}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_heilongjiang_gio = BashOperator(
        task_id="etl_heilongjiang_gio",
        bash_command=f"""
        {bedsv_cmd_heilongjiang}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_heilongjiang = BashOperator(
        task_id="export_heilongjiang",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 451 heilongjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_heilongjiang = BashOperator(
        task_id="push_heilongjiang",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/heilongjiang/
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} heilongjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_shandong = BashOperator(
        task_id="etl_shandong",
        bash_command=f"""
        {bedsv_cmd_shandong}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_shandong_gio = BashOperator(
        task_id="etl_shandong_gio",
        bash_command=f"""
        {bedsv_cmd_shandong}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_shandong = BashOperator(
        task_id="export_shandong",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 531 shandong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_shandong = BashOperator(
        task_id="push_shandong",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/shandong/
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} shandong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guangxi = BashOperator(
        task_id="etl_guangxi",
        bash_command=f"""
        {bedsv_cmd_guangxi}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guangxi_gio = BashOperator(
        task_id="etl_guangxi_gio",
        bash_command=f"""
        {bedsv_cmd_guangxi}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_guangxi = BashOperator(
        task_id="export_guangxi",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 771 guangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_guangxi = BashOperator(
        task_id="push_guangxi",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/guangxi/
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} guangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_anhui = BashOperator(
        task_id="etl_anhui",
        bash_command=f"""
        {bedsv_cmd_anhui}/h5/etl_ads_hachi_cm003_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_anhui_gio = BashOperator(
        task_id="etl_anhui_gio",
        bash_command=f"""
        {bedsv_cmd_anhui}/h5/etl_ads_hachi_cm003_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_anhui = BashOperator(
        task_id="export_anhui",
        bash_command=f"""
        sh  /home/udbac/bin/hachi_bigtable_prov_cm003.sh {dfmt} CM003 551 anhui
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_anhui = BashOperator(
        task_id="push_anhui",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM003/anhui/
        sh /home/udbac/bin/new_push_cm.sh CM003 {DT} anhui
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )


    etl_quanguo >> export_quanguo >> push_quanguo
    etl_liaoning >> etl_liaoning_gio >> export_liaoning >> push_liaoning
    etl_xinjiang >> etl_xinjiang_gio >> export_xinjiang >> push_xinjiang
    etl_guizhou >> etl_guizhou_gio >> export_guizhou >> push_guizhou
    etl_beijing >> etl_beijing_gio >> export_beijing >> push_beijing
    etl_jiangsu >> etl_jiangsu_gio >> export_jiangsu >> push_jiangsu
    etl_sichuan >> etl_sichuan_gio >> export_sichuan >> push_sichuan
    etl_jiangxi >> etl_jiangxi_gio >> export_jiangxi >> push_jiangxi
    etl_yunnan >> etl_yunnan_gio >> export_yunnan >> push_yunnan
    etl_guangdong >> etl_guangdong_gio >> export_guangdong >> push_guangdong
    etl_zhejiang >> etl_zhejiang_gio >> export_zhejiang >> push_zhejiang
    etl_ningxia >> etl_ningxia_gio >> export_ningxia >> push_ningxia
    etl_hebei >> etl_hebei_gio >> export_hebei >> push_hebei
    etl_henan >> etl_henan_gio >> export_henan >> push_henan
    etl_heilongjiang >> etl_heilongjiang_gio >> export_heilongjiang >> push_heilongjiang
    etl_shandong >> etl_shandong_gio >> export_shandong >> push_shandong
    etl_guangxi >> etl_guangxi_gio >> export_guangxi >> push_guangxi
    etl_anhui >> etl_anhui_gio >> export_anhui >> push_anhui


