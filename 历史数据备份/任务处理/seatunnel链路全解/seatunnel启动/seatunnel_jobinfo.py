
import pandas as pd
import os
import requests
import time


def dingding_post(content):
    post_url = "http://10.253.100.169:10080/robot/send?access_token=b99566defcdc6c565e930c95d6dd12f344b99b93af41eecc232d32840fbc8729"
    requests.post(post_url,json=content)


def get_jobinfo():
    global last_status
    os.popen("sh /data1/shanpf/get_seatunneljob.sh").read()
    time.sleep(3)
    df = pd.read_csv('/data1/shanpf/seatunnel_jobinfo.txt',header=None)
    res = list()
    for i in df[0].iloc[0].split(" "):
        if i != '':
            res.append(i)
    status = res[2]
    text = {"at": {"isAtAll": True},"text": {"content":"业务报警\n任务名称：seatunnel_cdpevent_hive\n任务状态：{}".format(status)},"msgtype":"text"}
    if status != last_status:
        dingding_post(text)
        if status == 'FAILED':
            print("任务失败")
            os.popen('''sshpass -p "IYEaWpx3wbj46qYjZhxw" ssh -o "StrictHostKeyChecking no" hdfs@seatunnel-1 "nohup /usr/local/seatunnel-2.3.3/bin/seatunnel.sh -r 801839845868568584 --config /usr/local/seatunnel-2.3.3/config/kafka2hive.conf -n cdpevent_hive > nohup.log 2>&1 &"''').read()
    last_status = status


if __name__ == '__main__':
    last_status = ''
    while True:
        get_jobinfo()
        time.sleep(300)


ssh app@seatunnel-10 "/usr/local/seatunnel-2.3.3/bin/seatunnel.sh -l |grep 801839845868568584" > /data1/shanpf/seatunnel_jobinfo.txt

nohup /usr/local/seatunnel-2.3.3/bin/seatunnel.sh  --config /usr/local/seatunnel-2.3.3/config/kafka2hive.conf -n cdpevent_hive > nohup.log 2>&1 &