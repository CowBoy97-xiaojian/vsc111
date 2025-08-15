

# -*- coding: UTF-8 -*-
"""
  Author:  shanpeifeng --<>
  Purpose: Seatunnel_monitor
  Created: 2024/4/26 17:59
"""


import datetime
import os
import requests
import time

#任务信息
job_id = {
    'Seatunnel-Hive-newudf1':'836170467365617672',
    'Seatunnel-Kafka1':'836170981650202632',
}


job_name = {
    'Seatunnel-Hive-newudf1':'hive_newudf',
    'Seatunnel-Kafka1':'kafka_yfkafka',
}

config_file = {
    'Seatunnel-Hive-newudf1':'kafka2hive_text.conf',
    'Seatunnel-Kafka1':'kafka2kafka.conf',
}

def dingding_post(content):
    post_url = "http://10.253.100.169:10080/robot/send?access_token=b99566defcdc6c565e930c95d6dd12f344b99b93af41eecc232d32840fbc8729"
    requests.post(post_url,json=content)



def get_topiclag(consumer_id):
    lag_res = list()
    partition_lag = dict()
    lag_info = os.popen('''sshpass -p "IYEaWpx3wbj46qYjZhxw" ssh -o "StrictHostKeyChecking no" app@10.104.81.166 "/data1/apps/lib/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 10.104.81.166:9092 --describe --group {}|grep cdp-event-collect"'''.format(consumer_id)).read()
    for lines in lag_info.split("\n"):
        if lines != '':
            for lag in lines.split(" "):
                if lag != '':
                    lag_res.append(lag)
            partition_lag[lag_res[0]+"-"+lag_res[1]] = int(lag_res[4])
            lag_res.clear()
    max_lag = max(partition_lag.values())
    max_partition = max(partition_lag, key=partition_lag.get)
    if max_lag >= 1000000:
        job_info = os.popen('''sshpass -p "IYEaWpx3wbj46qYjZhxw" ssh -o "StrictHostKeyChecking no" hdfs@seatunnel-1 "/usr/local/seatunnel-2.3.3/bin/seatunnel.sh -l |grep {}"'''.format(job_id[consumer_id])).read()
        state_res = list()
        for i in job_info.split(" "):
            if i != '':
                state_res.append(i)
        status = state_res[2]
        text_lag = {"at": {"isAtAll": True},
                    "text": {"content": "Kafka消费积压告警\nSeatunnel任务名称：{}\nSeatunnel任务状态：{}\n消费组名称：{}\n最大积压分区：{}\n最大积压lag值：{}".format(job_name[consumer_id],status,consumer_id,str(max_partition),str(max_lag))},
                    "msgtype": "text"}
        dingding_post(text_lag)
        if status == "RUNNING":
            os.popen('''sshpass -p "IYEaWpx3wbj46qYjZhxw" ssh -o "StrictHostKeyChecking no" hdfs@seatunnel-1 "/usr/local/seatunnel-2.3.3/bin/seatunnel.sh -s {}"'''.format(job_id[consumer_id])).read()
            print("当前时间_"+str(datetime.datetime.now())+"：保存checkpoint")
            time.sleep(60)
        os.popen('''sshpass -p "IYEaWpx3wbj46qYjZhxw" ssh -o "StrictHostKeyChecking no" hdfs@seatunnel-1 "nohup /usr/local/seatunnel-2.3.3/bin/seatunnel.sh -r {} --config /usr/local/seatunnel-2.3.3/config/{} -n {} > /usr/local/seatunnel-2.3.3/nohup_{}.log 2>&1 &"'''.format(job_id[consumer_id],config_file[consumer_id],job_name[consumer_id],job_name[consumer_id])).read()
        print("当前时间_"+str(datetime.datetime.now()) + "：重新启动任务")
        time.sleep(500)



# def get_jobinfo(consumer_id):
#     global last_status
#     job_info = os.popen('''sshpass -p "IYEaWpx3wbj46qYjZhxw" ssh -o "StrictHostKeyChecking no" hdfs@seatunnel-1 "/usr/local/seatunnel-2.3.3/bin/seatunnel.sh -l |grep {}"'''.format(job_id[consumer_id])).read()
#     state_res = list()
#     for i in job_info.split(" "):
#         if i != '':
#             state_res.append(i)
#     status = state_res[2]
#     text = {"at": {"isAtAll": True},"text": {"content":"业务报警\n任务名称：seatunnel_cdpevent_hive\n任务状态：{}".format(status)},"msgtype":"text"}
#     if status != last_status:
#         dingding_post(text)
#         if status != 'RUNNING':
#             print("任务失败")
#             os.popen('''sshpass -p "IYEaWpx3wbj46qYjZhxw" ssh -o "StrictHostKeyChecking no" hdfs@seatunnel-1 "nohup /usr/local/seatunnel-2.3.3/bin/seatunnel.sh -r 807161795872555016 --config /usr/local/seatunnel-2.3.3/config/kafka2hive.conf -n cdpevent_hive > /usr/local/seatunnel-2.3.3/nohup.log 2>&1 &"''').read()
#     last_status = status



if __name__ == '__main__':
    while True:
        get_topiclag('Seatunnel-Hive-newudf1')
        time.sleep(60)