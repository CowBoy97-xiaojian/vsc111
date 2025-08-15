#!/bin/bash

# ClickHouse连接信息
HOST="localhost"
PORT="8123"
USER="default"
PASSWORD=""
# 查询和插入的表名
SOURCE_TABLE="source_table"
TARGET_TABLE="target_table"
# 查询语句
QUERY="SELECT * FROM $SOURCE_TABLE"
# 每批次导出的数据量（行数）
BATCH_SIZE=100000
# 导出到本地的临时文件
OUTPUT_FILE="/tmp/query_output.csv"

# 确保输出文件不存在
rm -f $OUTPUT_FILE

# 获取查询结果的总行数
TOTAL_ROWS=$(clickhouse-client -h $HOST -p $PORT --user $USER --password $PASSWORD -q "SELECT count() FROM ($QUERY) AS tmp")
echo "总行数: $TOTAL_ROWS"

# 根据每批次的数据量，计算分批次查询的次数
BATCH_COUNT=$((($TOTAL_ROWS + $BATCH_SIZE - 1) / $BATCH_SIZE))
echo "分批次查询次数: $BATCH_COUNT"

# 分批次查询并插入数据
for ((i = 0; i < $BATCH_COUNT; i++))
do
    # 分页查询语句
    OFFSET=$(($i * $BATCH_SIZE))
    PAGING_QUERY="$QUERY LIMIT $BATCH_SIZE OFFSET $OFFSET"
    echo "查询语句: $PAGING_QUERY"

    # 导出数据到本地临时文件
    clickhouse-client -h $HOST -p $PORT --user $USER --password $PASSWORD -q "$PAGING_QUERY FORMAT CSV" > $OUTPUT_FILE

    # 插入数据到目标表
    clickhouse-client -h $HOST -p $PORT --user $USER --password $PASSWORD -q "INSERT INTO $TARGET_TABLE FORMAT CSV" < $OUTPUT_FILE

    # 清空临时文件
    rm -f $OUTPUT_FILE
done

echo "数据插入完成"