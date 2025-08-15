
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
        {becsv_cmd}/rpt_event_detail_d.hql | sed '/\\r/d' | sed '/^$/d' >> 全渠道汇总表_{ffmt}.CP.csv
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

    sftp_haoka = BashOperator(
        task_id="sftp_haoka",
        bash_command=f"""{dir_cmd}
        gzip -9f 0001_00*_{dfmt}_*.txt
        md5sum 0001_00*_{dfmt}_*.txt.gz | {md5awk_cmd} | gzip -9fc > 0001_{dfmt}_CHK.gz
        {lftp_cmd}/chama-270/chamadata/ -e "
        mkdir -p {dfmt}; cd {dfmt}; mput -e 0001_00*_{dfmt}_*.txt.gz 0001_{dfmt}_CHK.gz; exit" """,
        dag=dag,
    )

    rpt_guoqing_sum_ch_pg = BashOperator(
        task_id="rpt_guoqing_sum_ch_pg",
        bash_command=f"""{dir_cmd}
        echo "序号,业务,页面,事件,page_id,channel_id,PV,UV" > 国庆节活动报表channelid_pageid_{ffmt}.csv
        {becsv_cmd}/rpt_guoqing_sum_ch_pg.hql | sed '/\\r/d' | sed '/^$/d' >> 国庆节活动报表channelid_pageid_{ffmt}.csv """,
        dag=dag,
    )

    rpt_guoqing_sum = BashOperator(
        task_id="rpt_guoqing_sum",
        bash_command=f"""{dir_cmd}
        echo "序号,业务,页面,事件,PV,UV" > 国庆节活动报表_汇总_{ffmt}.csv
        {becsv_cmd}/rpt_guoqing_sum.hql | sed '/\\r/d' | sed '/^$/d' >> 国庆节活动报表_汇总_{ffmt}.csv """,
        dag=dag,
    )

    sftp_qycs = BashOperator(
        task_id="sftp_qycs",
        bash_command=f"""{dir_cmd}
        {lftp_cmd}/cocfiles/cm-to-qycs/ -e "
        mkdir -p {dfmt}; cd {dfmt}; mput -e dwd_ts* ; exit" """,
        dag=dag,
    )

    rpt_dwd_ts_mk_block_cul = BashOperator(
        task_id="rpt_dwd_ts_mk_block_cul",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,page_id,page_name,page_url,component_id,component_name,block_id,block_name,pv_exp,uv_exp,pv_cli,uv_cli,uv_login_cli" > dwd_ts_mk_block_cul_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_ts_mk_block_cul.hql | sed '/\\r/d' | sed '/^$/d' >> dwd_ts_mk_block_cul_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_block_cul_by_channel = BashOperator(
        task_id="rpt_dwd_ts_mk_block_cul_by_channel",
        bash_command=f"""{dir_cmd}
        echo "p_date_cd,page_id,page_name,page_url,component_id,component_name,block_id,block_name,pv_exp,uv_exp,pv_cli,uv_cli,uv_login_cli,market2_id,touch_code" > dwd_ts_mk_block_cul_by_channel_{dfmt}.csv
	{becsv_cmd}/rpt_dwd_ts_mk_block_cul_by_channel.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_block_cul_by_channel_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_block_cul_by_vip = BashOperator(
        task_id="rpt_dwd_ts_mk_block_cul_by_vip",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,page_id,page_name,page_url,component_id,component_name,block_id,block_name,pv_exp,uv_exp,pv_cli,uv_cli,uv_login_cli,is_vip" > dwd_ts_mk_block_cul_by_vip_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_ts_mk_block_cul_by_vip.hql | sed '/\\r/d' | sed '/^$/d' >> dwd_ts_mk_block_cul_by_vip_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_block_cul_by_channel_vi = BashOperator(
        task_id="rpt_dwd_ts_mk_block_cul_by_channel_vi",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,page_id,page_name,page_url,component_id,component_name,block_id,block_name,pv_exp,uv_exp,pv_cli,uv_cli,uv_login_cli,market2_id,touch_code,is_vip" > dwd_ts_mk_block_cul_by_channel_vi_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_ts_mk_block_cul_by_channel_vi.hql | sed '/\\r/d' | sed '/^$/d' >> dwd_ts_mk_block_cul_by_channel_vi_{dfmt}.csv
         """,
        dag=dag,
    )


    rpt_dwd_ts_mk_page_cul = BashOperator(
        task_id="rpt_dwd_ts_mk_page_cul",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,page_id,page_name,page_url,pv_bro,uv_bro,uv_usr" > dwd_ts_mk_page_cul_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_ts_mk_page_cul.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_page_cul_{dfmt}.csv
         """,
        dag=dag,
     )

    rpt_dwd_ts_mk_page_cul_by_channel_vi = BashOperator(
        task_id="rpt_dwd_ts_mk_page_cul_by_channel_vi",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,page_id,page_name,page_url,pv_bro,uv_bro,uv_usr,market2_id,touch_code,is_vip" >  dwd_ts_mk_page_cul_by_channel_vi_{dfmt}.csv
	{becsv_cmd}/rpt_dwd_ts_mk_page_cul_by_channel_vi.hql | sed '/\\r/d' | sed '/^$/d' >> dwd_ts_mk_page_cul_by_channel_vi_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_page_cul_by_channel = BashOperator(
        task_id="rpt_dwd_ts_mk_page_cul_by_channel",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,page_id,page_name,page_url,pv_bro,uv_bro,uv_usr,market2_id,touch_code" > dwd_ts_mk_page_cul_by_channel_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_ts_mk_page_cul_by_channel.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_page_cul_by_channel_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_page_cul_by_vip = BashOperator(
        task_id="rpt_dwd_ts_mk_page_cul_by_vip",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,page_id,page_name,page_url,pv_bro,uv_bro,uv_usr,is_vip" > dwd_ts_mk_page_cul_by_vip_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_ts_mk_page_cul_by_vip.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_page_cul_by_vip_{dfmt}.csv
         """,
        dag=dag,
    ) 

    rpt_dwd_ts_mk_posi_cul = BashOperator(
        task_id="rpt_dwd_ts_mk_posi_cul",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,page_id,page_name,page_url,component_id,component_name,block_id,block_name,posi_id,posi_name,pv_exp,uv_exp,pv_cli,uv_cli,uv_login_cli,location_link" > dwd_ts_mk_posi_cul_{dfmt}.csv
	{becsv_cmd}/rpt_dwd_ts_mk_posi_cul.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_posi_cul_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_posi_cul_by_channel_vi = BashOperator(
        task_id="rpt_dwd_ts_mk_posi_cul_by_channel_vi",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,page_id,page_name,page_url,component_id,component_name,block_id,block_name,posi_id,posi_name,pv_exp,uv_exp,pv_cli,uv_cli,uv_login_cli,location_link,market2_id,touch_code,is_vip" > dwd_ts_mk_posi_cul_by_channel_vi_{dfmt}.csv
	{becsv_cmd}/rpt_dwd_ts_mk_posi_cul_by_channel_vi.hql | sed '/\\r/d' | sed '/^$/d' >> dwd_ts_mk_posi_cul_by_channel_vi_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_posi_cul_by_channel = BashOperator(
        task_id="rpt_dwd_ts_mk_posi_cul_by_channel",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,page_id,page_name,page_url,component_id,component_name,block_id,block_name,posi_id,posi_name,pv_exp,uv_exp,pv_cli,uv_cli,uv_login_cli,location_link,market2_id,touch_code" > dwd_ts_mk_posi_cul_by_channel_{dfmt}.csv
	{becsv_cmd}/rpt_dwd_ts_mk_posi_cul_by_channel.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_posi_cul_by_channel_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_posi_cul_by_vip = BashOperator(
        task_id="rpt_dwd_ts_mk_posi_cul_by_vip",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,page_id,page_name,page_url,component_id,component_name,block_id,block_name,posi_id,posi_name,pv_exp,uv_exp,pv_cli,uv_cli,uv_login_cli,location_link,is_vip" > dwd_ts_mk_posi_cul_by_vip_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_ts_mk_posi_cul_by_vip.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_posi_cul_by_vip_{dfmt}.csv
         """,
        dag=dag,
    )


    rpt_dwd_ts_mk_total_cul = BashOperator(
        task_id="rpt_dwd_ts_mk_total_cul",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,pv_bro,uv_bro,uv_usr,uv_bro_detail,pv_pay,uv_pay" > dwd_ts_mk_total_cul_{dfmt}.csv
	{becsv_cmd}/rpt_dwd_ts_mk_total_cul.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_total_cul_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_total_cul_by_channel = BashOperator(
        task_id="rpt_dwd_ts_mk_total_cul_by_channel",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,pv_bro,uv_bro,uv_usr,uv_bro_detail,pv_pay,uv_pay,market2_id,touch_code" > dwd_ts_mk_total_cul_by_channel_{dfmt}.csv
	{becsv_cmd}/rpt_dwd_ts_mk_total_cul_by_channel.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_total_cul_by_channel_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_total_cul_by_vip = BashOperator(
        task_id="rpt_dwd_ts_mk_total_cul_by_vip",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,pv_bro,uv_bro,uv_usr,uv_bro_detail,pv_pay,uv_pay,is_vip" > dwd_ts_mk_total_cul_by_vip_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_ts_mk_total_cul_by_vip.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_total_cul_by_vip_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_total_cul_by_channel_vi = BashOperator(
        task_id="rpt_dwd_ts_mk_total_cul_by_channel_vi",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,pv_bro,uv_bro,uv_usr,uv_bro_detail,pv_pay,uv_pay,market2_id,touch_code,is_vip" > dwd_ts_mk_total_cul_by_channel_vi_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_ts_mk_total_cul_by_channel_vi.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_total_cul_by_channel_vi_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_goods_pay_vis = BashOperator(
        task_id="rpt_dwd_ts_mk_goods_pay_vis",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,member_id,pv_bro,uv_bro,uv_login_user,pv_pay,uv_pay" > dwd_ts_mk_member_cul_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_ts_mk_goods_pay_vis.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_member_cul_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_ts_mk_goods_pay_vis_vi = BashOperator(
        task_id="rpt_dwd_ts_mk_goods_pay_vis_vi",
        bash_command=f"""{dir_cmd}
	echo "p_date_cd,member_id,pv_bro,uv_bro,uv_login_user,pv_pay,uv_pay,is_vip" > dwd_ts_mk_member_cul_by_vip_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_ts_mk_goods_pay_vis_vi.hql | sed '/\\r/d' | sed '/^$/d'  >> dwd_ts_mk_member_cul_by_vip_{dfmt}.csv
         """,
        dag=dag,
    )

    rpt_dwd_xinliyu_di = BashOperator(
        task_id="rpt_dwd_xinliyu_di",
        bash_command=f"""{dir_cmd}
        FNAME=zunxiangliyu_{dfmt}.csv
        {becsv_cmd}/rpt_dwd_xinliyu_di.hql > $FNAME
        {lftp_cmd}/zunxiangliyu -e "mput -c $FNAME ; exit;"
        """,
        dag=dag,
    )

    join_haoka = DummyOperator(task_id='join_haoka', trigger_rule="none_failed", dag=dag)
    join_qycs = DummyOperator(task_id='join_qycs', trigger_rule="none_failed", dag=dag)
    
    rpt_jtqs_di = BashOperator(
        task_id="rpt_jtqs_di",
        bash_command=f"""{dir_cmd}
        FNAME=jtqs_{dfmt}.csv
        echo "no,pv,uv" > $FNAME   
        {becsv_cmd}/rpt_jtqs_di.hql | sed '/\\r/d' | sed '/^$/d' >> $FNAME
        {lftp_cmd}/jtqs -e "mput -c $FNAME ; exit;"
        """,
        dag=dag
    )

    rpt_lyy = BashOperator(
        task_id="rpt_lyy",
        bash_command=f"""{dir_cmd}
        echo "日期,四级渠道编码,首页访问PV,首页访问UV,订单量,手机号码输入框点击PV,手机号码输入框点击UV,获取验证码按钮点击PV,获取验证码按钮点击UV,1元限时体验按钮点击PV,1元限时体验按钮点击UV,确认办理按钮点击PV,确认办理按钮点击UV,订购成功弹窗PV,订购成功弹窗UV,订购失败弹窗PV,订购失败弹窗UV,去兑换按钮点击PV,去兑换按钮点击UV,复制按钮点击PV,复制按钮点击UV" > lyy_{dfmt}.csv
        {becsv_cmd}/rpt_lyy.hql | sed '/\\r/d' | sed '/^$/d' >> lyy_{dfmt}.csv
        {lftp_cmd}/ -e "
        mkdir -p lyy_{dfmt}; cd lyy_{dfmt}; mput -e lyy_{dfmt}.csv; exit" """,
        dag=dag,
    )

    rpt_lyydd = BashOperator(
        task_id="rpt_lyydd",
        bash_command=f"""{dir_cmd}
        echo "日期,四级渠道编码,号码归属省份,订单量" > lyydd_{dfmt}.csv
        {becsv_cmd}/rpt_lyydd.hql | sed '/\\r/d' | sed '/^$/d' >> lyydd_{dfmt}.csv
        {lftp_cmd}/ -e "
        mkdir -p lyy_{dfmt}; cd lyy_{dfmt}; mput -e lyydd_{dfmt}.csv; exit" """,
        dag=dag,
    )

    rpt_ads_carnot_fsuvzh_detail_di = BashOperator(
        task_id="rpt_ads_carnot_fsuvzh_detail_di",
        bash_command=f"""{dir_cmd}
        echo "日期,手机号" > fsuvzh_detail_{dfmt}.csv
        {becsv_cmd}/rpt_ads_carnot_fsuvzh_detail_di.hql | sed '/\\r/d' | sed '/^$/d' >> fsuvzh_detail_{dfmt}.csv
        {lftp_cmd}/fsuvzh -e "
        mkdir -p fsuvzh_detail_{dfmt}; cd fsuvzh_detail_{dfmt}; mput -e fsuvzh_detail_{dfmt}.csv; exit" """,
        dag=dag,
    )

    rpt_ads_carnot_fsuvzh_di = BashOperator(
        task_id="rpt_ads_carnot_fsuvzh_di",
        bash_command=f"""{dir_cmd}
        echo "日期,省份,page_id,channel_id,分组id,满足前置条件uv,结果条件uv,转化率" > fsuvzh_{dfmt}.csv
        {becsv_cmd}/rpt_ads_carnot_fsuvzh_di.hql | sed '/\\r/d' | sed '/^$/d' >> fsuvzh_{dfmt}.csv
        {lftp_cmd}/fsuvzh -e "
        mkdir -p fsuvzh_{dfmt}; cd fsuvzh_{dfmt}; mput -e fsuvzh_{dfmt}.csv; exit" """,
        dag=dag,
    )

    rpt_ads_ts_mo_pr_ci_di = BashOperator(
        task_id="rpt_ads_ts_mo_pr_ci_di",
        bash_command=f"""{dir_cmd}
        echo "日期,手机号码,省份,城市,设备系统,新用户,日活跃用户周活跃用户,月活跃用户,回流用户,累计用户,沉默用户,留存用户" > yhxxb.csv
        {becsv_cmd}/rpt_ads_ts_mo_pr_ci_di.hql | sed '/\\r/d' | sed '/^$/d' >> yhxxb.csv
        {lftp_cmd}/qycsyhbq -e "
        mkdir -p {dfmt}; cd {dfmt}; mput -e yhxxb.csv; exit" """,
        dag=dag,
    )

    rpt_ads_ts_mo_avgspent = BashOperator(
        task_id="rpt_ads_ts_mo_avgspent",
        bash_command=f"""{dir_cmd}
        echo "日期,sessionid,手机号码,停留时长" > zttlscmxb.csv
        {becsv_cmd}/rpt_ads_ts_mo_avgspent.hql | sed '/\\r/d' | sed '/^$/d' >> zttlscmxb.csv
        {lftp_cmd}/qycsyhbq -e "
        mkdir -p {dfmt}; cd {dfmt}; mput -e zttlscmxb.csv; exit" """,
        dag=dag,
    )

    rpt_ads_ts_avgspent = BashOperator(
        task_id="rpt_ads_ts_avgspent",
        bash_command=f"""{dir_cmd}
        echo "日期,停留时长" > ztpjtlscb.csv
        {becsv_cmd}/rpt_ads_ts_avgspent.hql | sed '/\\r/d' | sed '/^$/d' >> ztpjtlscb.csv
        {lftp_cmd}/qycsyhbq -e "
        mkdir -p {dfmt}; cd {dfmt}; mput -e ztpjtlscb.csv; exit" """,
        dag=dag,
    )

    rpt_ads_ts_pa_sp_di = BashOperator(
        task_id="rpt_ads_ts_pa_sp_di",
        bash_command=f"""{dir_cmd}
        echo "日期,页面id,当前页面url,停留时长,页面退出率,页面跳出率" > ymxxb.csv
        {becsv_cmd}/rpt_ads_ts_pa_sp_di.hql | sed '/\\r/d' | sed '/^$/d' >> ymxxb.csv
        {lftp_cmd}/qycsyhbq -e "
        mkdir -p {dfmt}; cd {dfmt}; mput -e ymxxb.csv; exit" """,
        dag=dag,
    )

    rpt_ads_ts_mo_sp_di = BashOperator(
        task_id="rpt_ads_ts_mo_sp_di",
        bash_command=f"""{dir_cmd}
        echo "日期,sessionid,手机号码,页面id,当前页面url,停留时长" > ymtlscmxb.csv
        {becsv_cmd}/rpt_ads_ts_mo_sp_di.hql | sed '/\\r/d' | sed '/^$/d' >> ymtlscmxb.csv
        {lftp_cmd}/qycsyhbq -e "
        mkdir -p {dfmt}; cd {dfmt}; mput -e ymtlscmxb.csv; exit" """,
        dag=dag,
    )


    rpt_event_detail_d
    rpt_event_detail_d2y
    rpt_haoka_00100_d >> join_haoka
    rpt_haoka_00101_d >> join_haoka
    rpt_haoka_00200_d >> join_haoka 
    rpt_haoka_00300_d >> join_haoka
    rpt_haoka_00400_d >> join_haoka
    rpt_haoka_00500_d >> join_haoka
    join_haoka >> sftp_haoka
    rpt_guoqing_sum_ch_pg
    rpt_guoqing_sum
    rpt_dwd_xinliyu_di
    rpt_dwd_ts_mk_block_cul >> join_qycs
    rpt_dwd_ts_mk_block_cul_by_channel >> join_qycs
    rpt_dwd_ts_mk_block_cul_by_vip >> join_qycs
    rpt_dwd_ts_mk_block_cul_by_channel_vi >> join_qycs
    rpt_dwd_ts_mk_page_cul >> join_qycs
    rpt_dwd_ts_mk_page_cul_by_channel_vi >> join_qycs
    rpt_dwd_ts_mk_page_cul_by_channel >> join_qycs
    rpt_dwd_ts_mk_page_cul_by_vip >> join_qycs
    rpt_dwd_ts_mk_posi_cul >> join_qycs
    rpt_dwd_ts_mk_posi_cul_by_channel_vi >> join_qycs 
    rpt_dwd_ts_mk_posi_cul_by_channel >> join_qycs
    rpt_dwd_ts_mk_posi_cul_by_vip >> join_qycs
    rpt_dwd_ts_mk_total_cul >> join_qycs
    rpt_dwd_ts_mk_total_cul_by_channel >> join_qycs
    rpt_dwd_ts_mk_total_cul_by_vip >> join_qycs
    rpt_dwd_ts_mk_total_cul_by_channel_vi >> join_qycs
    rpt_dwd_ts_mk_goods_pay_vis >> join_qycs
    rpt_dwd_ts_mk_goods_pay_vis_vi >> join_qycs
    join_qycs >> sftp_qycs
    rpt_jtqs_di
    rpt_lyy
    rpt_lyydd
    rpt_ads_carnot_fsuvzh_detail_di
    rpt_ads_carnot_fsuvzh_di
    rpt_ads_ts_mo_pr_ci_di
    rpt_ads_ts_mo_avgspent
    rpt_ads_ts_avgspent
    rpt_ads_ts_pa_sp_di
    rpt_ads_ts_mo_sp_di