sql查询语句.sql

select * from ods;

select mobile, 
case when logon_t1='0' and (logon_t2='1' or logon_t3 = '1')then '低粘度用户'
when xxx
when xxx
end as user_type,
from
(select 
	mobile,
	max(T-1.tag) over(partitions mobile,T-1) logon_t1,
	max(T-2.tag) over(partitions mobile,T-2) logon_t2,
	max(T-3.tag) over(partitions mobile,T-3) logon_t3
from XXX
where data_time
group by mobile
)tb

