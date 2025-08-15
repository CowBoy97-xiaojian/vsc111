
--根据实时写文件到本地后，取第一行时间和最后一行时间，到event_all中查询对应时间的一个数据量
select count(1) from olap.event_all where toDateTime(event_time) between '2023-06-22 13:37:07' and '2023-06-22 13:48:59' and data_source_id = 'dcsu6g4ik05bnyopi0vkfs0799s_3l2h';



2023-06-23 01:55:14   
2023-06-23 10:14:29
clickhouse ---5102730
online     ---5096066
select count(1) from olap.event_all where toDateTime(event_time) between '2023-06-23 09:55:14' and '2023-06-23 10:14:29' and data_source_id = 'dcsu6g4ik05bnyopi0vkfs0799s_3l2h';