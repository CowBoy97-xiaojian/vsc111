--权益超市51010
select "210315_QYCS_12313" REGEXP "^210315_QYCS_\\d+$";
--权益超市51006
select "210315_QYCS_qwqqeq" REGEXP "^210315_QYCS_[a-zA-Z]+$";
where mc_ev = "210315_QYCS"
--一级权益超市实时渠道

权益口径单独调整
先处理有mc_ev的数据,再处理没有mc_ev的数据(通过domain关联)。

domain 

210315_QYCS_12321


210315_QYCS_dadq
210315_QYCS

实时51006口径
1、取mc_ev 包含"210315_QYCS"就不关联domain,反之关联。
2、
"210315_QYCS_qwqqeq" REGEXP "^210315_QYCS_[a-zA-Z]+$";
or mc_ev = "210315_QYCS"
中金融科技-H5	853d176d98834573


51010
1、取mc_ev 包含"210315_QYCS"
2、"210315_QYCS_12313" REGEXP "^210315_QYCS_\\d+$";
中金融科技-H5	853d176d98834573

现51006的口径是
--51006口径
渠道：
集运H5
微信小程序
字节、阿里小程序
微信公众号
轻渠道开发活动类H5
采集轻渠道微门户H5
外部渠道室外投放H5
一级权益超市H5
支付宝
字节H5
京东小程序
APP轻载-小程序
轻渠道新电商H5
轻渠道视频直播H5

加上domain关联

--51006权益超市口径
权益超市的口径
一级权益超市H5+权益超市的domain

51006-gio
1、对('a1f48d9ff4f42571','b508a809cbbddd0b')两个渠道数据中 (mc_ev is null
    or (instr(mc_ev,'210315_QYCS')=0 and instr(mc_ev,'210902_QYLQTYY')=0))这部分数据进行domain维表关联
2、对('a1f48d9ff4f42571','b508a809cbbddd0b')两个渠道数据中(((instr(mc_ev,'210315_QYCS') > 0 and !mc_ev REGEXP '^210315_QYCS_\\d+$')
            or instr(mc_ev,'210902_QYLQTYY')>0))这部分数据不关联domain维表，获取这部分全量数据

51010-gio

1、对('a1f48d9ff4f42571','b508a809cbbddd0b')两个渠道数据中 (mc_ev is null
    or (instr(mc_ev,'210315_QYCS')=0 and instr(mc_ev,'210902_QYLQTYY')=0))这部分数据进行domain维表关联
2、对('a1f48d9ff4f42571','b508a809cbbddd0b')两个渠道数据中(mc_ev REGEXP '^210315_QYCS_\\d+$')这部分数据不关联domain维表，获取这部分全量数据
