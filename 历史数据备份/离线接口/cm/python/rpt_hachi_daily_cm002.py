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
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=quanguo --hivevar PROV_ID=000  --hivevar DCSID=90be4403373b6463  -f /home/udbac/hqls/"""

bedsv_cmd_shanghai = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=shanghai --hivevar PROV_ID=210  --hivevar DCSID=bdf908bd8e07b82c  -f /home/udbac/hqls/"""

bedsv_cmd_chongqing = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=chongqing --hivevar PROV_ID=230  --hivevar DCSID=b00057b79cbf85af  -f /home/udbac/hqls/"""

bedsv_cmd_xinjiang = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=xinjiang --hivevar PROV_ID=991  --hivevar DCSID=9e488aa27d948855  -f /home/udbac/hqls/"""

bedsv_cmd_guizhou = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=guizhou --hivevar PROV_ID=851 --hivevar DCSID=98e2f7b831f876dd  -f /home/udbac/hqls/"""

bedsv_cmd_beijing = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=beijing --hivevar PROV_ID=100 --hivevar DCSID=86596eaccd0d746a  -f /home/udbac/hqls/"""

bedsv_cmd_jiangsu = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=jiangsu --hivevar PROV_ID=250 --hivevar DCSID=94bbabc3a9686c5a  -f /home/udbac/hqls/"""

bedsv_cmd_sichuan = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=sichuan --hivevar PROV_ID=280 --hivevar DCSID=a9ae56608c62f805  -f /home/udbac/hqls/"""

bedsv_cmd_jiangxi = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=jiangxi --hivevar PROV_ID=791 --hivevar DCSID=9ead359aaf617556  -f /home/udbac/hqls/"""

bedsv_cmd_yunnan = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=yunnan --hivevar PROV_ID=871 --hivevar DCSID=aebed7d26ca2d38a  -f /home/udbac/hqls/"""

bedsv_cmd_guangdong = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=guangdong --hivevar PROV_ID=200 --hivevar DCSID=8aeb9b26885f3d8b  -f /home/udbac/hqls/"""

bedsv_cmd_zhejiang = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=zhejiang --hivevar PROV_ID=571 --hivevar DCSID=be5412a41f02e47a  -f /home/udbac/hqls/"""

bedsv_cmd_ningxia = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=ningxia --hivevar PROV_ID=951 --hivevar DCSID=ac34c865ecb163fa  -f /home/udbac/hqls/"""

bedsv_cmd_hebei = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=hebei --hivevar PROV_ID=311 --hivevar DCSID=ad7c40e8ac8a0983  -f /home/udbac/hqls/"""

bedsv_cmd_henan = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=henan --hivevar PROV_ID=371 --hivevar DCSID=9328255238347f80  -f /home/udbac/hqls/"""

bedsv_cmd_heilongjiang = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=heilongjiang --hivevar PROV_ID=451 --hivevar DCSID=b1b4618c1d4fac12  -f /home/udbac/hqls/"""

bedsv_cmd_shandong = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=shandong --hivevar PROV_ID=531 --hivevar DCSID=aa4dbfdc0e193192  -f /home/udbac/hqls/"""

bedsv_cmd_guangxi = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=guangxi --hivevar PROV_ID=771 --hivevar DCSID=a20bffba73210972  -f /home/udbac/hqls/"""

bedsv_cmd_anhui = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT} --hivevar PROV=anhui --hivevar PROV_ID=551 --hivevar DCSID=af82bebd8421abec  -f /home/udbac/hqls/"""


default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

with DAG('rpt_hachi_daily_cm002',
         default_args=default_args,
         catchup=False,
         max_active_tasks=8,
         max_active_runs=7,
         schedule_interval='56 1 * * *'
         ) as dag:

    etl_yunnan = BashOperator(
        task_id="etl_yunnan",
        bash_command=f"""
        {bedsv_cmd_yunnan}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_yunnan_gio = BashOperator(
        task_id="etl_yunnan_gio",
        bash_command=f"""
        {bedsv_cmd_yunnan}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_yunnan = BashOperator(
        task_id="export_yunnan",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 871 yunnan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_yunnan = BashOperator(
        task_id="push_yunnan",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/yunnan
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} yunnan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_jiangxi = BashOperator(
        task_id="etl_jiangxi",
        bash_command=f"""
        {bedsv_cmd_jiangxi}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_jiangxi_gio = BashOperator(
        task_id="etl_jiangxi_gio",
        bash_command=f"""
        {bedsv_cmd_jiangxi}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_jiangxi = BashOperator(
        task_id="export_jiangxi",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 791 jiangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_jiangxi = BashOperator(
        task_id="push_jiangxi",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/jiangxi
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} jiangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_sichuan = BashOperator(
        task_id="etl_sichuan",
        bash_command=f"""
        {bedsv_cmd_sichuan}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_sichuan_gio = BashOperator(
        task_id="etl_sichuan_gio",
        bash_command=f"""
        {bedsv_cmd_sichuan}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_sichuan = BashOperator(
        task_id="export_sichuan",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 280 sichuan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_sichuan = BashOperator(
        task_id="push_sichuan",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/sichuan
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} sichuan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_jiangsu = BashOperator(
        task_id="etl_jiangsu",
        bash_command=f"""
        {bedsv_cmd_jiangsu}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_jiangsu_gio = BashOperator(
        task_id="etl_jiangsu_gio",
        bash_command=f"""
        {bedsv_cmd_jiangsu}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_jiangsu = BashOperator(
        task_id="export_jiangsu",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 250  jiangsu
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_jiangsu = BashOperator(
        task_id="push_jiangsu",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/jiangsu
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} jiangsu
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_beijing = BashOperator(
        task_id="etl_beijing",
        bash_command=f"""
        {bedsv_cmd_beijing}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_beijing_gio = BashOperator(
        task_id="etl_beijing_gio",
        bash_command=f"""
        {bedsv_cmd_beijing}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_beijing = BashOperator(
        task_id="export_beijing",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 100  beijing
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_beijing = BashOperator(
        task_id="push_beijing",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/beijing
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} beijing
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guizhou = BashOperator(
        task_id="etl_guizhou",
        bash_command=f"""
        {bedsv_cmd_guizhou}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guizhou_gio = BashOperator(
        task_id="etl_guizhou_gio",
        bash_command=f"""
        {bedsv_cmd_guizhou}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_guizhou = BashOperator(
        task_id="export_guizhou",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 851  guizhou
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_guizhou = BashOperator(
        task_id="push_guizhou",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/guizhou
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} guizhou
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_xinjiang = BashOperator(
        task_id="etl_xinjiang",
        bash_command=f"""
        {bedsv_cmd_xinjiang}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_xinjiang_gio = BashOperator(
        task_id="etl_xinjiang_gio",
        bash_command=f"""
        {bedsv_cmd_xinjiang}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_xinjiang = BashOperator(
        task_id="export_xinjiang",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 991 xinjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_xinjiang = BashOperator(
        task_id="push_xinjiang",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/xinjiang
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} xinjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_shanghai = BashOperator(
        task_id="etl_shanghai",
        bash_command=f"""
        {bedsv_cmd_shanghai}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_shanghai_gio = BashOperator(
        task_id="etl_shanghai_gio",
        bash_command=f"""
        {bedsv_cmd_shanghai}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_shanghai = BashOperator(
        task_id="export_shanghai",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov.sh {dfmt} CM002 210 shanghai
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_shanghai = BashOperator(
        task_id="push_shanghai",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/shanghai
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} shanghai
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_chongqing = BashOperator(
        task_id="etl_chongqing",
        bash_command=f"""
        {bedsv_cmd_chongqing}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_chongqing_gio = BashOperator(
        task_id="etl_chongqing_gio",
        bash_command=f"""
        {bedsv_cmd_chongqing}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_chongqing = BashOperator(
        task_id="export_chongqing",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 230 chongqing
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_chongqing = BashOperator(
        task_id="push_chongqing",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/chongqing
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} chongqing
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guangdong = BashOperator(
        task_id="etl_guangdong",
        bash_command=f"""
        {bedsv_cmd_guangdong}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guangdong_gio = BashOperator(
        task_id="etl_guangdong_gio",
        bash_command=f"""
        {bedsv_cmd_guangdong}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_guangdong = BashOperator(
        task_id="export_guangdong",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 200 guangdong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_guangdong = BashOperator(
        task_id="push_guangdong",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/guangdong
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} guangdong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_zhejiang = BashOperator(
        task_id="etl_zhejiang",
        bash_command=f"""
        {bedsv_cmd_zhejiang}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_zhejiang_gio = BashOperator(
        task_id="etl_zhejiang_gio",
        bash_command=f"""
        {bedsv_cmd_zhejiang}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_zhejiang = BashOperator(
        task_id="export_zhejiang",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 571 zhejiang
#        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002_test.sh {dfmt} CM002 571 zhejiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_zhejiang = BashOperator(
        task_id="push_zhejiang",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/zhejiang
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} zhejiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_ningxia = BashOperator(
        task_id="etl_ningxia",
        bash_command=f"""
        {bedsv_cmd_ningxia}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_ningxia_gio = BashOperator(
        task_id="etl_ningxia_gio",
        bash_command=f"""
        {bedsv_cmd_ningxia}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_ningxia = BashOperator(
        task_id="export_ningxia",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 951 ningxia
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_ningxia = BashOperator(
        task_id="push_ningxia",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/ningxia
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} ningxia
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_quanguo = BashOperator(
        task_id="etl_quanguo",
        bash_command=f"""
#        {bedsv_cmd_quanguo}/jituan/etl_ads_hachi_cm002_prov_000.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_quanguo = BashOperator(
        task_id="export_quanguo",
        bash_command=f"""
#        sh  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 000 quanguo
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_quanguo = BashOperator(
        task_id="push_quanguo",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/quanguo/
#        lftp -u 'sftp_coc270,SFtp_cOc270!' -p 3964 sftp://10.252.180.2/incoming/cmpt/ -e "mput -c *.gz; mput -c *.verf; exit;"
        echo "push success"
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )




    etl_hebei = BashOperator(
        task_id="etl_hebei",
        bash_command=f"""
        {bedsv_cmd_hebei}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_hebei_gio = BashOperator(
        task_id="etl_hebei_gio",
        bash_command=f"""
        {bedsv_cmd_hebei}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_hebei = BashOperator(
        task_id="export_hebei",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 311 hebei
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_hebei = BashOperator(
        task_id="push_hebei",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/hebei
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} hebei
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_henan = BashOperator(
        task_id="etl_henan",
        bash_command=f"""
        {bedsv_cmd_henan}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_henan_gio = BashOperator(
        task_id="etl_henan_gio",
        bash_command=f"""
        {bedsv_cmd_henan}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_henan = BashOperator(
        task_id="export_henan",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 371 henan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_henan = BashOperator(
        task_id="push_henan",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/henan
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} henan
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_heilongjiang = BashOperator(
        task_id="etl_heilongjiang",
        bash_command=f"""
        {bedsv_cmd_heilongjiang}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_heilongjiang_gio = BashOperator(
        task_id="etl_heilongjiang_gio",
        bash_command=f"""
        {bedsv_cmd_heilongjiang}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_heilongjiang = BashOperator(
        task_id="export_heilongjiang",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 451 heilongjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_heilongjiang = BashOperator(
        task_id="push_heilongjiang",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/heilongjiang
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} heilongjiang
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_shandong = BashOperator(
        task_id="etl_shandong",
        bash_command=f"""
        {bedsv_cmd_shandong}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_shandong_gio = BashOperator(
        task_id="etl_shandong_gio",
        bash_command=f"""
        {bedsv_cmd_shandong}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_shandong = BashOperator(
        task_id="export_shandong",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 531 shandong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_shandong = BashOperator(
        task_id="push_shandong",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/shandong
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} shandong
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guangxi = BashOperator(
        task_id="etl_guangxi",
        bash_command=f"""
        {bedsv_cmd_guangxi}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_guangxi_gio = BashOperator(
        task_id="etl_guangxi_gio",
        bash_command=f"""
        {bedsv_cmd_guangxi}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_guangxi = BashOperator(
        task_id="export_guangxi",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 771 guangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_guangxi = BashOperator(
        task_id="push_guangxi",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/guangxi
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} guangxi
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_anhui = BashOperator(
        task_id="etl_anhui",
        bash_command=f"""
        {bedsv_cmd_anhui}/jituan/etl_ads_hachi_cm002_prov.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    etl_anhui_gio = BashOperator(
        task_id="etl_anhui_gio",
        bash_command=f"""
        {bedsv_cmd_anhui}/jituan/etl_ads_hachi_cm002_prov_gio.hql
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    export_anhui = BashOperator(
        task_id="export_anhui",
        bash_command=f"""
        sh -x  /home/udbac/bin/hachi_bigtable_prov_cm002.sh {dfmt} CM002 551 anhui
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push_anhui = BashOperator(
        task_id="push_anhui",
        bash_command=f"""
        cd /home/udbac/output/af_hachi_daily/{dfmt}/CM002/anhui
        sh /home/udbac/bin/new_push_cm.sh CM002 {DT} anhui
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )



    etl_quanguo >> export_quanguo >> push_quanguo
    etl_shanghai >> etl_shanghai_gio >> export_shanghai >> push_shanghai
    etl_chongqing >> etl_chongqing_gio >> export_chongqing >> push_chongqing
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

