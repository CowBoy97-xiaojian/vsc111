#!/bin/bash

# 进行告警信息发送的方法
send_alert() {
  local title=$1
  local host=$2
  local status=$3
  local info=$4
  local eventId=$5
  
  curl -X POST -H "Content-Type: application/json" http://logs-api01:19000/onlinesms -d @- <<EOF
  {
    "title": "$title",
    "eventId": "$eventId",
    "level": "4",
    "status": "$status",
    "info": "$info",
    "host": "$host",
    "time": "$(date -d'10 minutes ago' +'%Y-%m-%d %H:%M:%S')",
    "lastTime": "$(date +'%Y-%m-%d %H:%M:%S')"
  }
EOF
}

# 定义获取错误数量的函数
get_error_count() {
  local host=$1
  command="awk -v d1=\"\$(date -d'10 minutes ago' +'%Y/%m/%d %H:%M:%S')\" -v d2=\"\$(date +'%Y/%m/%d %H:%M:%S')\" 'BEGIN { gsub(/[-:]/,\"/\",d1); gsub(/[-:]/,\"/\",d2); } \$0 > d1 && \$0 < d2 || \$0 ~ d2 {if (\$0 ~ /error/) count++} END {print count}' /usr/local/openresty/nginx/logs/error.log"
  result=$(ssh -q app@$host "$command")
echo ${result:-0}
##  if [ $? -eq 0 ]; then
##    echo ${result:-0}
#  else
#    echo -1
#    return -1
  #fi
}



# 定义主机名
hosts=(prod-nginx-{1..10})

# 定义告警标题、状态和信息等常量
alert_title="--插码平台【软负载异常】告警--"
problem_status="PROBLEM"
ok_status="OK"
problem_info=""
ok_info="ngingx 错误 已恢复，运行正常"


# 初始化检查参数
err_limit=1     #设置错误最高值
chk_pred=10     #设置检查周期
eventId=""      #上报的事件id
last_error_status=0     # 最后一次错误的状态
alert_sent=0            # 是否已经够发送过告警
have_error=0            # 是否有错误，用来触发告警
error_count=0           # 错误统计数字

# 开始执行while条件进行对主机的监控
while true; do
        # 对所有主机进行轮询
        for host in  ${hosts[@]}; do
                echo "$date 执行【$host】的错误日志检查"
                # 获取过去十分钟内的错误日志数量
                error_count=$(get_error_count $host)
echo "........... $error_count ................."
##检测主机程序是否发生错误，log为 空
#               if (( error_count = -1 )); then
#                      # 生成告警信息，并根据故障主机进行累加
#                       problem_info="$problem_info 主机$host 在 $chk_pred 分钟进行err检查时发生错误退出.\n"
#                       # 设置是否有告警的标志
#                       have_error=1
#               fi
                echo "$date ----【$host】的错误日志中err数量为$error_count"
                # 如果该主机的错误数量大于告警条件
                if (( error_count >= err_limit )); then
                        # 生成告警信息，并根据故障主机进行累加
                        problem_info="$problem_info 主机$host 在 $chk_pred 分钟内产生err错误$error_count 个.\n"
                        # 设置是否有告警的标志
                        have_error=1
                fi
        done

        # 如果错误数量超有告警标志，发送告警
        if (( have_error == 1 )); then
                # 如果eventId为空，生成一个新的eventId
                if [ -z "$eventId" ]; then
                        eventId=$(date +%s)
                fi
                # 发送告警
                send_alert $alert_title "openResty" $problem_status "$problem_info" "$eventId"
                # 处于告警状态，且已经发送告警
                last_error_status=1
                alert_sent=1
                # 本次告警完成，告警标志设置为0，为下次检查准备
                have_error=0
        else
                # 如果没有错误，且上一个时间段也没有错误，且已经发送过告警，发送恢复告警
                if (( last_error_status == 0 && alert_sent == 1 )); then
                        send_alert $alert_title "$host" $ok_status  $ok_info "$eventId"
                        # 清空eventId和告警发送标志
                        eventId=""
                        alert_sent=0
                fi
                last_error_status=0
        fi
      
        # 按照检查周期进行检查检查一次
        sleep $((chk_pred*60))
done