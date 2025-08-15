with DAG('rpt_51008_daily',
         default_args=default_args,
         catchup=False,
         schedule_interval='05 08 * * *'
         ) as dag:

    get_ld_workorder = BashOperator(
        task_id="get_ld_workorder",
        bash_command=f"""
        sh -x /home/udbac/bin/get_ld_workorder.sh {dfmt}
        """,
        dag=dag
    )

    ld_ord_info = BashOperator(
        task_id="ld_ord_info",
        bash_command=f"""
        sh -x /home/udbac/bin/ld_ord_info.sh {dfmt}
        """,
        dag=dag
    )

    hachi_jzyy_xtb2_51008 = BashOperator(
        task_id="hachi_jzyy_xtb2_51008",
        bash_command=f"""
        {bedsv_cmd}/jituan/etl_ads_hachi_jzyy_xtb2_51008.hql
        """,
        dag=dag,
	max_active_tis_per_dag=1
    )

    export = BashOperator(
        task_id="export",
        bash_command=f"""
        cd /home/udbac/output/51008_daily/
        sh  /home/udbac/bin/hachi_bigtable_07-08.sh {dfmt} JZYY_XTB2_51008
        """,
        dag=dag,
        max_active_tis_per_dag=1
    )

    push = BashOperator(
        task_id="push",
        bash_command=f"""
        cd /home/udbac/output/51008_daily/{dfmt}/
	sh  /home/udbac/bin/push_06-08.sh
        ct=$(ls *.dat.gz | wc -l)
        sh /home/udbac/bin/write_count_51.sh 51008 {DT} $ct
        """,
        dag=dag
    )

get_ld_workorder >> ld_ord_info >> hachi_jzyy_xtb2_51008 >> export >> push