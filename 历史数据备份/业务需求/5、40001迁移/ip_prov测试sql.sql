and daytime is not null

 select 
 tb2.ip_prov,count(1) 
 from webtrends.event_hi_dcslog_all tb1 inner join ham.dim_prov_code_all tb2 on tb1.ip_city=tb2.ip_prov
 where 
 dt = '2023-06-20' and mc_ev='210315_QYCS' and daytime is not null
 group by tb2.ip_prov;