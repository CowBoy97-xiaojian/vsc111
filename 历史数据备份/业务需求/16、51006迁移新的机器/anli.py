from airflow.operators.dummy import DummyOperator
from pendulum import timezone
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable




DT = '{{ execution_date.add(hours=8).to_date_string() }}'  # 

dfmt = '{{ execution_date.add(hours=8).strftime("%Y%m%d") }}'
HH = '{{ execution_date.add(hours=8).strftime("%H") }}'


default_args = {
    'owner': 'udbac',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8, tzinfo=timezone("Asia/Shanghai")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}



with DAG('flink_rpt_51010_copy_hour',
         default_args=default_args,
         catchup=False,
         schedule_interval='10 * * * *'
         ) as dag:


    copy_flink_51010 = BashOperator(
        task_id="copy_flink_51010",
        bash_command=f"""
        rm -rf /root/output_ck/51010_daily/{DT}/*_{HH}.dat.gz
        sh /root/bin/file_move.sh 51010 {DT}/{HH} {DT} {HH}
        """,
        dag=dag,
    )