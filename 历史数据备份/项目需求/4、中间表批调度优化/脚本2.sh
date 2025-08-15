
#!/bin/bash

# ClickHouse connection information
host="localhost"
port="8123"
user="default"
password=""
database="default"

# Query to get data from source table
sql_query="SELECT * from source_table WHERE condition"

# Batch insert size
batch_size=10000

# Destination table name and columns
destination_table="destination_table"
destination_columns="(col1, col2, col3)"

# Connect to ClickHouse database
clickhouse-client \
--host "$host" \
--port "$port" \
--user "$user" \
--password "$password" \
--database "$database" \
--query "INSERT INTO $destination_table $destination_columns VALUES"

# Query data from source table in batches
clickhouse-client \
--host "$host" \
--port "$port" \
--user "$user" \
--password "$password" \
--database "$database" \
--format_csv_no_header \
--max_block_size="$batch_size" \
--query "$sql_query" \
| while read line; do
  # Append data to insert query
  echo -n "($line)," >> insert_query.txt

  # Execute insert query when batch size is reached
  if ((${LINENO} % $batch_size == 0)); then
    insert_query=$(<insert_query.txt sed 's/,$//')
    insert_query="$insert_query;"
    clickhouse-client \
    --host "$host" \
    --port "$port" \
    --user "$user" \
    --password "$password" \
    --database "$database" \
    --query "${insert_query}"
    rm insert_query.txt
    touch insert_query.txt
  fi  
done

# Insert remaining data
if [ -s insert_query.txt ]; then
  insert_query=$(<insert_query.txt sed 's/,$//')
  insert_query="$insert_query;"
  clickhouse-client \
  --host "$host" \
  --port "$port" \
  --user "$user" \
  --password "$password" \
  --database "$database" \
  --query "${insert_query}"
  rm insert_query.txt
fi