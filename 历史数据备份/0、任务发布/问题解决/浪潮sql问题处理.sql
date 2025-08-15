
select count(1)
from (
    select * 
    from (
        select 
        STATIS_DATE,
        CASE WHEN ADVERTYPE = '--' THEN '' ELSE ADVERTYPE END AS ADVERTYPE,
        CASE WHEN wt_et = '--' THEN ''  else wt_et end AS wt_et,
        trmnl_style 
        from ods.O_51007_CLIENT_INTERPOLATION_ORIGINAL_LOG_D ) a
    where  STATIS_DATE= '20240521' 
    and trmnl_style in ('a1f48d9ff4f42571', 'IOS','ANDROID','b508a809cbbddd0b')
    and trmnl_style in ('a1f48d9ff4f42571', 'IOS','ANDROID','b508a809cbbddd0b')
    and WT_ET in ('clk','imp')
    and advertype = ''
    ) b