select
toDate(event_time) as dt,
count(1) as pv,
count(distinct anonymous_user) uv
from olap.event_all
where dt between '2023-03-29' and '2023-04-29'
group by dt
order by dt
FORMAT CSVWithNames;