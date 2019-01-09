@author: kikyoar  
@contact: nokikyoar@gmail.com  


此程序配置支持Zabbix3.0及以上版本  
python版本要求3.6
需要安装urlib3库
此程序调用NIFI API


# 监控项如下：
	- NIFI bulidTag
	- NIFI版本
	- NIFI集群daemonThreads
	- NIFI集群totalHeap
	- NIFI集群totalNonHeap
	- NIFI集群usedHeap
	- NIFI集群usedNonHeap
	- NIFI集群使用空间大小
	- NIFI集群处理器负载
	- NIFI集群当前在Process Group中排队的FlowFiles数目
	- NIFI集群总的空间大小
	- NIFI集群总的线程数
	- NIFI集群总的节点数量
	- NIFI集群活动的线程数量activeThreadCount
	- NIFI集群节点异常输出情况
	- NIFI集群连接的NIFI节点数量
	- process group 
		- flowFilesIn
		- flowFilesOut
		- flowFilesQueued
		- queuedSize
		- errorMessages（文本）
		- connections （文本，依据官方文档61%正常，61%-85%拥挤，85%以上阻塞）
			- queuedSize
			- queuedCount



# 程序配置办法：  

	- 此程序因部署在可以与NIFI集群网络可达的环境中
	- 上传程序文件夹"NIFI_Monitor“至/home/zabbix目录下	
		- 修改代码中的url字段IP地址和端口
		# NIFI API
		url = "http://127.0.0.1:18088/nifi-api/"
	- 修改并添加zabbix_agentd.conf配置文件
		- AllowRoot=1
		- UnsafeUserParameters=1
		- UserParameter=ProcessGrouplist,python3 /home/zabbix/NIFI_Monitor/ClusterProcessList.py
	    - UserParameter=ProcessGroupcheck[*],python3 /home/zabbix/NIFI_Monitor/ProcessGroupAttributes.py $1 $2
	    - UserParameter=Maincheck[*],python3 /home/zabbix/NIFI_Monitor/MainMonitor.py $1
		- Timeout=30
	- 修改并添加zabbix_server.conf配置文件
		- Timeout=30
	- 重启zabbix_agentd，zabbix_server
	- 测试
		部署程序所在服务器：
			zabbix_agentd -t ProcessGrouplist 
			如果以上命令有返回值，说明正常
	- 以上操作结束，在zabbix页面中导入模板即可
	   注意：此模板为配置触发器，请根据项目实际情况配置