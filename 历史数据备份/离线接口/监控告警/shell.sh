[dwzhangjianchao@carnot01 opt]$ cat jiankong_nginx.sh  
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

send_alert "--插码平台【软负载异常】告警--" "host" "PROBLEM" "插码平台数据检验测试，测试数据链路是否是通路，测试时间是11.24.15，测试人：张建超" "1700811214"

    "title": "--信息告警--",
    "eventId": "1700811214",
    "level": "4",
    "status": "PROBLEM",
    "info": "插码平台数据检验测试，测试数据链路是否是通路，测试时间是11.24.15，测试人：张建超",
    "host": "11111",

# 定义获取错误数量的函数
get_error_count() {
  local host=$1
  command="awk -v d1=\"\$(date -d'180 minutes ago' +'%Y/%m/%d %H:%M:%S')\" -v d2=\"\$(date +'%Y/%m/%d %H:%M:%S')\" 'BEGIN { gsub(/[-:]/,\"/\",d1); gsub(/[-:]/,\"/\",d2); } \$0 > d1 && \$0 < d2 || \$0 ~ d2 {if (\$0 ~ /error/) count++} END {print count}' /usr/local/openresty/nginx/logs/error.log"
  ssh $host "$command"
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
err_limit=50    #设置错误最高值
chk_pred=600    #设置检查周期
eventId=""      #上报的事件id
last_error_status=0     # 最后一次错误的状态
alert_sent=0            # 是否已经够发送过告警
have_error=0            # 是否有错误，用来触发告警

# 开始执行while条件进行对主机的监控
while true; do
        # 对所有主机进行轮询
        for host in  ${hosts[@]}; do
                # 获取过去十分钟内的错误日志数量
                error_count=$(get_error_count $host)
                # 如果该主机的错误数量大于告警条件
                if (( error_count >= err_limit )); then
                        # 生成告警信息，并根据故障主机进行累加
                        problem_info=problem_info+"$host在$chk_pred分钟内产生err$err_count个.\n"
                        # 设置是否有告警的标志
                        have_error=1
                fi
        done
      
        # 如果错误数量超有告警标志，发送告警
        if (( have_error = 1 )); then
                # 如果eventId为空，生成一个新的eventId
                if [ -z "$eventId" ]; then
                        eventId=$(date +%s)
                fi
                # 发送告警
                send_alert $alert_title "openResty" $problem_status  $alert_title$problem_info "$eventId"
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
        sleep $chk_pred
done



curl -X POST -H "Content-Type: application/json" http://logs-api01:19000/onlinesms -d @- <<EOF
  {
    "title": "--信息告警--",
    "eventId": "1700811214",
    "level": "4",
    "status": "PROBLEM",
    "info": "插码平台数据检验测试，测试数据链路是否是通路，测试时间是11.24.15，测试人：张建超",
    "host": "11111",
    "time": "$(date -d'10 minutes ago' +'%Y-%m-%d %H:%M:%S')",
    "lastTime": "$(date +'%Y-%m-%d %H:%M:%S')"
  }
  EOF

2023-11-24 15:56:24,616 - my_logger - INFO - Received data: {'title': '--插码平台【软负载异常】告警--', 'eventId': '1700811214', 'level': '4', 'status': 'OK', 'info': '插码平台数据检验测试，测试数据链路是否是通路，测试时间是11.24.15，测试人：张建超', 'host': 'host', 'time': '2023-11-24 15:46:24', 'lastTime': '2023-11-24 15:56:24'}

2023-11-24 15:56:24,676 - my_logger - INFO - Forwarded data: {'to': '', 'type': '1', 'title': '--插码平台【软负载异常】告警--', 'message': {'eventId': '1700811214', 'level': '4', 'status': 'OK', 'info': '插码平台数据检验测试，测试数据链路是否是通路，测试时间是11.24.15，测试人：张建超', 'host': 'host', 'time': '2023-11-24 15:46:24', 'lastTime': '2023-11-24 15:56:24', 'nettype': '业务进程', 'item': '', 'image': '', 'currentValue': '', 'firstAlarmType': '', 'secondAlarmType': '', 'province': '', 'version': '', 'costCenter': '', 'business': '', 'application': '', 'project': ''}}. Received response: 200
2023-11-24 15:56:24,676 - my_logger - INFO - Forwarded finish ok:200


2023-11-24 15:57:22,710 - my_logger - INFO - Received data: {'title': '--插码平台【软负载异常】告警--', 'eventId': '1700811214', 'level': '4', 'status': 'OKoo', 'info': '插码平台数据检验测试，测试数据链路是否是通路，测试时间是11.24.15，测试人：张建超', 'host': 'host', 'time': '2023-11-24 15:47:22', 'lastTime': '2023-11-24 15:57:22'}


2023-11-24 15:57:22,775 - my_logger - INFO - Forwarded data: {'to': '', 'type': '1', 'title': '--插码平台【软负载异常】告警--', 'message': {'eventId': '1700811214', 'level': '4', 'status': 'OKoo', 'info': '插码平台数据检验测试，测试数据链路是否是通路，测试时间是11.24.15，测试人：张建超', 'host': 'host', 'time': '2023-11-24 15:47:22', 'lastTime': '2023-11-24 15:57:22', 'nettype': '业务进程', 'item': '', 'image': '', 'currentValue': '', 'firstAlarmType': '', 'secondAlarmType': '', 'province': '', 'version': '', 'costCenter': '', 'business': '', 'application': '', 'project': ''}}. Received response: 200
2023-11-24 15:57:22,775 - my_logger - INFO - Forwarded finish ok:200