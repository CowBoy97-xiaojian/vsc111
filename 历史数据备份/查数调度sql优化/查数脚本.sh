#! /bin/bash
SQL=$1
outputfile=$2
dt=$3

date=$(date  +%Y%m%d%HH%mm%ss)

echo "$date \n" >> $outputfile

for hh in {00,01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23}
do
beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac --hivevar DT=$dt --hivevar HH=$hh  -f "/home/udbac/zjc_demo/$SQL" | grep -vE "^$|\n" >> /home/udbac/zjc_demo/out_sql.out
done


select count(1)
from ham.dwd_dcslog_event_gio_di 
where 
dt='${DT}'
and hour = '${HH}';

SQL

51007-ods
select ${DT},${HH},count(1) as ods
from ham_jituan.ods_client_event_gio_all
where 
dt='${DT}'
and hour = '${HH}';

51006-ods
select ${DT},${HH},count(1) as ods
from ham.ods_dcslog_event_gio_all
where 
dt='${DT}'
and hour = '${HH}';


51007-dwd
select ${hour},count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='${dt}' 
and  hour ='${hour}';



51016-dwd





脚本优化
select 00,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-04' 
and  hour ='00'
union all
select 00,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-04' 
and  hour ='01'
union all
select 00,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-04' 
and  hour ='02'
union all
select 00,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-04' 
and  hour ='03'
union all
select 00,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-04' 
and  hour ='04'
union all
select 00,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-04' 
and  hour ='05';


55-57
select hour,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-01' 
and  hour in ('00','01','02','03','04','05')
group by hour
order by hour;
select hour,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-04' 
and  hour in ('06','07','08','09','10','11')
group by hour
order by hour;
select hour,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-04' 
and  hour in ('12','13','14','15','16','17')
group by hour
order by hour;
select hour,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-04' 
and  hour in ('18','19','20','21','22','23')
group by hour
order by hour;

select hour,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-01' 
and  hour in ('00','01','02','03','04','05','06','07')
group by hour
order by hour;

select hour,count(1) as dwd
from ham.dwd_dcslog_event_gio_di
where dt='2023-11-01' 
and  hour in ('00','01','02','03','04','05','06','07')
group by hour
order by hour;

('00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')

select hour,count(1) as odssum
from ham.ods_dcslog_event_gio_all
where dt='${DT}' 
and  hour in $HH
group by hour
order by hour;

if [ expression 1 ]
then
Statement(s) to be executed if expression 1 is true
elif [ expression 2 ]
then
Statement(s) to be executed if expression 2 is true
elif [ expression 3 ]
then
Statement(s) to be executed if expression 3 is true
else
Statement(s) to be executed if no expression is true
fi


for hh in {"('00','01','02','03','04','05','06','07')","('08','09','10','11','12','13','14','15')","('16','17','18','19','20','21','22','23')"}
do
beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac --hivevar DT=$dt --hivevar HH=$hh  -f "/home/udbac/zjc_demo/$SQL" | grep -vE "^$|\n" >> /home/udbac/zjc_demo/out_sql.out
done




57 02

select hour,sum(cnt) as cnt
from
(select hour,count(1) as cnt
from ham_jituan.dwd_client_event_gio_di 
where dt='2023-11-19'
and (
    trmnl_style in ('b95440ef47ec01fc','90be4403373b6463')
or
    (trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b')
    and wt_event in ('c_perf','FlowDataCollection')
    )
)
group by hour
order by hour
union all
select hour,count(1) as cnt
from ham_jituan.dwd_client_event_gio_di tb2
inner join (
select  distinct
domain,
interface
from ham.dim_domain_interface_di
where interface = '51007') tb4
on tb2.domain=tb4.domain
where dt='2023-11-19'
and trmnl_style in ('a1f48d9ff4f42571','b508a809cbbddd0b')
group by hour
order by hour
) tb1
group by hour
order by hour
;
