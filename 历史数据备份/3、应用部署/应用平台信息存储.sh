1、git地址

GIT仓库淮安生产桌面云内地址：
http://172.19.3.123:18176
测试桌面云git地址：
http://172.22.225.32:8804
GIT源地址：
http://192.168.26.176:8888

git账号
登录账号：panxiangfei
登陆账号：zhangjianchao
登录初始密码：1qaz!QAZ

2、测试openGuess数据库信息
    测试环境已创建完毕：
    IP：172.22.232.34、172.22.232.115、172.22.232.144
    端口：25400
    DB：qdzczx
    schema：cmplatform、auditplatform
    username：cmplatform、auditplatform
    passwd：1q2w!Q@W

3、测试环境数据库信息

云桌面环境
redis-ha-test-public-02-0.redis-ha-test-public-02.zjjpt-redis.svc.ha-double-hpc.local,redis-ha-test-public-02-1.redis-ha-test-public-02.zjjpt-redis.svc.ha-double-hpc.local,redis-ha-test-public-02-2.redis-ha-test-public-02.zjjpt-redis.svc.ha-double-hpc.local,redis-ha-test-public-02-3.redis-ha-test-public-02.zjjpt-redis.svc.ha-double-hpc.local,redis-ha-test-public-02-4.redis-ha-test-public-02.zjjpt-redis.svc.ha-double-hpc.local,redis-ha-test-public-02-5.redis-ha-test-public-02.zjjpt-redis.svc.ha-double-hpc.local

Redis测试库对应的账号及密码如下：
mcp123456
48245CD8D1E256CC346A3A3FBAC8D1E2

IP：172.22.232.34、172.22.232.115、172.22.232.144
    端口：25400
    DB：qdzczx
    schema：cmplatform、auditplatform
    username：cmplatform、auditplatform
    passwd：1q2w!Q@W

本地环境
su - app 
url: jdbc:opengauss://118.25.189.51:19543/ubc_manage?currentSchema=ubc
username: cm_app
password: JLqrkL!pmd*_*NcqKWyQ$y
gsql -U cm_app -d postgres -p 19543 -h 118.25.189.51

4、云桌面环境配置
http://frontend.cs.cmos/frontend/api/resources/view/content.html#/pc/initDocs  前端资源库地址
- 问题自主处理文档：https://www.kdocs.cn/l/cdJD6ex9Z9LO
- 客户端操作手册：https://www.kdocs.cn/l/cv4ltxTuoaOG


5、云桌面环境
软件
	navicat、idea、dbeaver、apipost
编程环境
	maven、node、nvm




用户-123456
管理员
admin

产品-新增方案
product_role

插码-审核方案
coded_role

测试信息填写-前段开发
fill_in_role


普通用户
ry

测试用户 
test


流程
产品-新增方案  =》 插码-审核方案 =〉 测试信息填写-前段开发 =》插码-核查（生成核查报告、明细）

通过 结束

不通过 测试信息填写-回到待核













6、本地测试环境

1，安装xshell或者crt远程工具 
2，安装 rzsz yum install -y lrzsz 
3，将包拖进去
4，useradd app 
mv openGauss-5.0.1-CentOS-64bit.tar.bz2  /home/app 
chown -R app:app /home/app 
su - app 
mkdir opengauss 
mv openGauss-5.0.1-CentOS-64bit.tar.bz2  opengauss 
cd  opengauss 
tar -xf openGauss-5.0.1-CentOS-64bit.tar.bz2

cd  openguass/simpleInstall/
bash install.sh  -w 'mima'





