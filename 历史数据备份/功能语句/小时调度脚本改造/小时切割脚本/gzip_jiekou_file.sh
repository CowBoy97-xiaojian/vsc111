#!/bin/bash

# 启用关联数组功能
shopt -s associative_arrays

declare -A params=(
    [operation_type]="SELECT"
    [dfmt]=""
    [jkbh]=""
    [dt]="$dt"
    [hour]="$hour"
    [start_time]="$start_time"
    [end_time]="$end_time"
    [file_count]="$export_file_sum"
    [export_sum]="$export_data_sum"
    [hive_sum]="$hive_data_sum"
    [data_consistency]="$data_consistency"
    [state]=""
)

params[dfmt]="$1"
params[jkbh]="$2"
params[dt]="$3"
params[Hour]="$4"

data_proc_folder="/data/output/51007_gio_hourly/${params[dfmt]}/${params[dfmt]}_${params[Hour]}"
hive_source_folder="/user/hive/warehouse/ham_jituan.db/ads_hachi_jzyy_xtb2_51007_gio_dt_hour/dt=${params[dt]}/hour=${params[Hour]}/*"
log_file=export_${params[dfmt]}_${params[jkbh]}_${params[hour]}.log

# 使用hive来记录任务执行状态
# "INSERT SELECT" "$dt" "$hour" "$start_time" "$end_time" "$file_count" "$export_sum" "$hive_sum" "$data_consistency" "$state"
function run_hive_query() {
    local -n params=$1  # 使用 nameref 参数接收关联数组引用

    if [[ "${params[operation_type]}" == "INSERT" ]]; then
        SQL="INSERT OVERWRITE TABLE ham_jituan.data_job_monitoring partition (dt = '${params[dt]}',hour = '${params[hour]}',job_name = 'export_51007_gio_hourly') values ('51007','${params[start_time]}','${params[end_time]}','${params[file_count]}','${params[export_sum]}','${params[hive_sum]}','${params[data_consistency]}','${params[state]}');"
        beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac  -e "$SQL"
    elif [[ "${params[operation_type]}" == "SELECT" ]]; then
        SQL="select count(1) from ham_jituan.ads_hachi_jzyy_xtb2_51007_gio_dt_hour where dt = '${params[dt]}' and hour = '${params[hour]}';"
        result=$(beeline --nullemptystring=true --showHeader=false --outputformat=csv2 --silent=true -u  jdbc:hive2://master01:10000/ham -n udbac  -e "$SQL")
        echo "$result"
    else
        echo "Invalid operation type: ${params[operation_type]}"
        exit 1
    fi

}

function get_hive_files() {
    local success=false # 拉取文件成功标志
    local tries=5   # 拉取文件尝试次数
    mkdir -p "$data_proc_folder"
    echo "创建小时文件夹: $data_proc_folder"
    for retry in $(seq 1 $tries); do
        hdfs dfs -get -dest "$data_proc_folder" "$hive_source_folder" && success=true
        # 如果命令执行成功且文件个数大于 0，则退出循环
        if [ "$success" = true ] ;then
            fc_orig=$(find "$data_proc_folder" -type f -name "00*.gz" | wc -l)
            if [ "$fc_orig" -gt 0 ]; then
                echo "获取原始数据文件个数：$fc_orig"
                break;
            else
                echo "命令执行成功，但未找到 .gz 文件"
                success=false
            fi
        fi
        echo "重新尝试拉取文件 $retry 次"
        fc_orig=0
        sleep 5
    done
}

# 定义任务的重试机制
function retry_tsk() {
    local success=false # 命令执行结果
    local tries=5   # 命令尝试尝试次数
    local comd_1=$1   # 具体要执行的命令1
    local comd_2=$2   # 具体要执行的命令2
    for retry in $(seq 1 $tries); do
        eval "$comd_1" && success=true  
        if [ "$success" = true ] ;then
            echo "$comd_1 执行结果: $success "
            eval "$comd_2" && success=true  
            if [ "$success" = true ] ; then  # 判断命令2的执行状态码是否为0（通常情况下，0表示成功）
                echo "$comd_2 执行结果: $success "
                break
            fi
        else
            echo "$comd_1 执行结果: $success ,进行重试 $retry "
        fi
        sleep 5
    done
}

# 多任务处理
function multi_processing_task() {
    local task_queue=("$@")     # 任务待执行队列 
    local parallelism=$2        # 任务执行的并行度
    local running_tasks=0       # 同时运行的任务计数器
    local child_pids=()         # 存储子进程的进程ID数组
    # 启动并行任务
    while true; do
        if [[ ${#task_queue[@]} -gt 0 && running_tasks -lt $parallelism ]]; then
            # 使用 IFS 分割字符串
            IFS=',' read -ra task <<< "${task_queue[0]}"
            (
                retry_tsk "${task[0]}" "${task[1]}"
            ) &
            child_pids+=($!)  # 将子进程的进程ID存储在数组中
            task_queue=("${task_queue[@]:1}")
            running_tasks+=1   # 增加正在运行的任务数
            echo "**** started ${task[0]} *******"
        else
            # 遍历 child_pids 数组
            for pid in "${child_pids[@]}"; do
                # 使用 ps 命令检查进程是否存在
                if ! ps -p "$pid" >/dev/null 2>&1; then
                    # 如果进程不存在，从 child_pids 数组中删除它
                    child_pids=("${child_pids[@]/$pid}")
                    running_tasks=$((running_tasks - 1))   # 递减正在运行的任务数
                fi
            done
            [[ ${#task_queue[@]} -eq 0 && $running_tasks -eq 0 ]] && break  # 如果队列为空且没有正在运行的任务，则退出循环   
        fi
    done
}

# 遍历要处理的文件列表，并加入任务执行队列
function decompression_hive_files(){
    local find_patition="00*.gz"
    declare -a task_queue=()

    find "$data_proc_folder" -type f -name "$find_patition" | sort  > temp_file
    while read -r file_path; do
        comd_1="gzip -dc $file_path | iconv -ct GBK | sed 's/\x23_\x23/\x80/g'  > $data_proc_folder/new_$file_path.txt "
        comd_2="rm -rf $file_path" 
        task_queue+=("${comd_1},${comd_2}")    # 将查找到的文件添加到待执行队列
    done < temp_file
    rm temp_file
    multi_processing_task "${task_queue[@]}" "20"  
}

#执行解压后的临时文件的处理 标记行号、分割
function split_exchange_file() {
    local split_file_patition=a_10000_"$dfmt"_"$jkbh"_"$Hour"_00_
    local split_parallelism=20
    local split_cnt=800000    
    # Todo 考虑经过替换后产生空行的问题
    comd_1="find $data_proc_folder -name 'new_*.txt' -print0 | xargs -0 -I {} -P $split_parallelism sh -c 'awk '\''NF {print \"00\" \"#_#\" $0}'\'' ' {} | split -l $split_cnt -d -a 4 --additional-suffix=.dat - $split_file_patition"

    comd_2="find $data_proc_folder -name new_*.txt -type f -delete"

    retry_tsk "$comd_1" "$comd_2"    
}

# 遍历要处理的文件列表，并加入任务执行队列
function compression_exchange_files(){
    local find_patition="*.dat"
    declare -a task_queue=()

    find "$data_proc_folder" -type f -name "$find_patition" | sort  > temp_file
    while read -r file_path; do
        comd_1="gzip -1f $file_path"
        comd_2="rm -rf $file_path"        
        task_queue+=("${comd_1},${comd_2}")    # 将查找到的文件添加到待执行队列
    done < temp_file
    rm temp_file
    multi_processing_task "${task_queue[@]}" "20"  
}

# 遍历要处理的文件列表，并加入任务执行队列
function verify_exchange_files(){
    local find_patition="*.dat.gz"
    local verify_file=a_10000_"$dfmt"_"$jkbh"_"$Hour"_00.verf
    # declare -a task_queue=()

    find "$data_proc_folder" -type f -name "$find_patition" | sort  > temp_file
    last_line=$(tail -n 1 temp_file)
    while read -r file_path; do
        filename=$(basename "$file_path")
        filesize=$(stat -c%s "$file_path")
        last_modified=$(stat -c%Y "$file_path")
        last_modified_date=$(date -d "$last_modified" +%Y%m%d%H%M%S)
        line_count=800000
        if [ "$file_path" = "$last_line" ]; then
            line_count=$(gzip -cd "$file_path" | wc -l)
        fi
        export_data_sum+=line_count # 计算文件总行数
        export_file_sum+=1
        echo "$filename,$filesize,$line_count,$dfmt,$last_modified_date" | iconv -t GBK | sed "s/\\x2c/\\x80/g" >> "$verify_file"
        # filename_gbk=$(echo "$filename" | iconv -t GBK | sed 's/\\x2c/\\x80/g')
        # comd_1="sed -i '/$filename_gbk/s/800000/$(gzip -cd $file_path | wc -l)/' $verify_file"
        # comd_2=""       
        # task_queue+=("${comd_1},${comd_2}")    # 将查找到的文件添加到待执行队列
    done < temp_file
    rm temp_file
    # multi_processing_task "${task_queue[@]}" "20"  
}

function check_hive_files(){
    params[hive_sum]=$(run_hive_query params )
    local export_dev=$((params[export_sum]-hive_data_sum))
    if [ $export_dev -ge -50 ] && [ $export_dev -le 50 ]; then
        data_consistency=true
    fi
    echo "$export_file_sum $hive_data_sum $export_dev $data_consistency" > "$log_file"          
}

function process_init(){
    params[state]="开始运行"
    run_hive_query "${params[@]}"
}

function process_final(){
    params[state]="运行完毕"
    run_hive_query "${params[@]}"
    # "INSERT" "$dt" "$hour" "$start_time" "$end_time" "$export_file_sum" "$export_data_sum" "$data_consistency" "运行完毕"
}

params[start_time]=$(date +"%Y-%m-%d %H:%M:%S")

echo "****** : 开始 ${params[dfmt]} ${params[jkbh]} ${params[dt]} ${params[hour]} 处理任务：${params[start_time]} "
process_init

echo "Step 1 : 拉取 $hive_source_folder"
# shellcheck disable=SC2078
if [ ! get_hive_files ]; then
    echo "拉取成功"
else
    echo "拉取失败"
    exit 1
fi

# to 如果 执行失败 直接 exit 1

echo "Step 2 : 对 $file_path 进行解压缩、字符集转换"
decompression_hive_files

echo "Step 3 : 对 $file_path 进行行号标记、分割文件"
split_exchange_file

echo "Step 4 : 对 $file_path 对切割后的文件执行压缩"
compression_exchange_files

echo "Step 5 : 对 $file_path 对压缩后的接口文件校验任务"
verify_exchange_files

echo "Step 6 : 对 $file_path 进行文件总行数的一致性检查"
check_hive_files

params[end_time]=$(date +"%Y-%m-%d %H:%M:%S")
echo "****** : 完成 ${params[dfmt]} ${params[jkbh]} ${params[dt]} ${params[hour]} 处理任务：${params[start_time]} - ${params[end_time]}"
process_final
