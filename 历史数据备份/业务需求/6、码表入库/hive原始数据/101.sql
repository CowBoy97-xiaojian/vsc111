select dt, province, event, page_id, channel_id, count(distinct ck_id) as uv
from (select dt,
             concat(split(event, "_")[2], "_", split(event, "_")[4], "_click") as event,
             if((b.code is null), '-2', b.code)                                as province,
             page_id,
             substr(if((channel_id like'C%' or channel_id like'P%'),substr(channel_id,0,12),channel_id),0,63) as channel_id,
             ck_id
      from dwd_dcslog_event_di a
               left join dim_province_code b on a.ip_prov = b.province
      where dt = '${DT}'
        and dcsid = '3w7x'
        and event rlike '20200423BZK_JHY_(tab|ljlq)_[0-9]+_[0-9]+') as all
group by event, dt, province, page_id, channel_id having uv > 0;