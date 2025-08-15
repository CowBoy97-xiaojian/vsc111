select dt,
       if((b.code is null), '-2', b.code) AS                                       province,
       page_id,
       substr(if((channel_id like'C%' or channel_id like'P%'),substr(channel_id,0,12),channel_id),0,63),
       count(distinct if(event in('20200429ZMKBLY_page','201204_XZFZMK29YTYY_JHY_SY_YMFWS'), ck_id, null))             AS home_page,
       count(distinct if(event like '20200429ZMKBLY_wybk_%' or event='201204_XZFZMK29YTYY_JHY_SY_DJLKAN', ck_id, null))        AS ljlq_click,
       count(distinct if(event = '20200423BZK_ZLTXY_page', ck_id, null))          AS info_page,
       count(distinct if(event = '20200423BZK_ZLTXY_name', ck_id, null))          AS name_input,
       count(distinct if(event = '20200423BZK_ZLTXY_sfz', ck_id, null))           AS sfz_input,
       count(distinct if(event = '20200423BZK_ZLTXY_phone', ck_id, null))         AS phone_input,
       count(distinct if(event = '20200423BZK_ZLTXY_hmgs', ck_id, null))          AS number_belong_click,
       count(distinct if(event = '20200423BZK_ZLTXY_xzhm', ck_id, null))          AS choose_number_click,
       count(distinct if(event = '20200423BZK_ZLTXY_szdq', ck_id, null))          AS location_click,
       count(distinct if(event = '20200423BZK_ZLTXY_psdz', ck_id, null))          AS address_input,
       count(distinct if(event = '20200423BZK_ZLTXY_ljtj', ck_id, null))          AS ljtj_click,
       count(distinct if(event = '20200423BZK_ZLTXY_OAO_suc_page', ck_id, null))  AS oao_success_page,
       count(distinct if(event = '20200423BZK_ZLTXY_MT_suc_page', ck_id, null))   AS mt_success_page,
       count(distinct if(event = '20200423BZK_ZLTXY_OAO_fail_page', ck_id, null)) AS oao_fail_page,
       count(distinct if(event = '20200423BZK_ZLTXY_MT_sl_page', ck_id, null))    AS mt_busying_page
from dwd_dcslog_event_di a left join dim_province_code b on a.ip_prov = b.province
where dt = '${DT}' and dcsid = '3w7x' 
group by dt, b.code, page_id, if((channel_id like'C%' or channel_id like'P%'),substr(channel_id,0,12),channel_id)
  having (home_page > 0 or
  ljlq_click > 0 or
  info_page > 0 or
  name_input > 0 or
  sfz_input > 0 or
  phone_input > 0 or
  number_belong_click > 0 or
  choose_number_click > 0 or
  location_click > 0 or
  address_input > 0 or
  ljtj_click > 0 or
  oao_success_page > 0 or
  mt_success_page > 0 or
  oao_fail_page > 0 or
  mt_busying_page > 0);