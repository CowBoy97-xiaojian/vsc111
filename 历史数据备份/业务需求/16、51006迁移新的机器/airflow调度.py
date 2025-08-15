from airflow.operators.dummy import DummyOperator
from pendulum import timezone
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable


HS2 = 'jdbc:hive2://master01:10000/ham'

DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # hive 分区字段dt

bedsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv \
                    -u {HS2} -n udbac --hivevar DT={DT}  -f /root/hqls"""

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'


HS2 = 'jdbc:hive2://master01:10000/ham'

DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # hive 分区字段dt

bedsv_cmd = f"""beeline --nullemptystring=true --showHeader=false --outputformat=dsv  -u jdbc:hive2://master01:10000/ham -n udbac --hivevar DT={DT}  -f /root/hqls"""

default_args = {
    'owner': 'udbac',
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG('hive_rpt_51006_daily',
         default_args=default_args,
         catchup=False,
         schedule_interval='56 0 * * *'
         ) as dag:

    hive_sql_51006 = BashOperator(
        task_id="hive_sql_51006",
        bash_command=f"""
        {bedsv_cmd}/51006_hive/etl_ads_rpt_hachi_51006.hql
        """,
        dag=dag,
        max_active_tis_per_dag = 1
    )

    hive_export = BashOperator(
        task_id="hive_export",
        bash_command=f"""
        sh  /root/hqls/51006_hive/hachi_bigtable_06.sh {dfmt}
        """,
        dag=dag,
        max_active_tis_per_dag = 1
    )

    hive_push = BashOperator(
        task_id="hive_push",
        bash_command=f"""
        cd /data5/output/51006_daily/{dfmt}
        sh  /root/hqls/51006_hive/push_06-08.sh
        ct=$(ls *.dat.gz | wc -l)
        sh /root/hqls/51006_hive/write_count_51.sh 51006 {DT} $ct
        """,
        dag=dag
    )
hive_sql_51006 >> hive_export >> hive_push



/root/hqls/51006_hive
/data5/output/51006_daily