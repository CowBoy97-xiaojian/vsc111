

例如导出数据到csv文件：
clickhouse-client -h 127.0.0.1 --database="defalut" --query="select * from
t_stock FORMAT CSV" > t_stock.csv

从csv文件导入数据：
clickhouse-client -h 127.0.0.1 --database="default" --query="insert into
t_stock FORMAT CSV" < ./test.csv

clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --format_csv_delimiter=$'|' --query="insert into ham.dim_order_detail FORMAT CSV" < ./a.csv



clickhouse-client -h 10.253.248.73 -m --database="ham" --receive_timeout=3600 --query="insert into
ham.dim_order_detail" < ./b.txt.gz
