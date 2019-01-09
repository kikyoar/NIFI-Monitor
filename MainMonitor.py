#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

@author: kikyoar
@contact: nokikyoar@gmail.com
@time: 2018/12/28 16:57

"""

import sys
import urllib3
import json
import re

http = urllib3.PoolManager()
# NIFI API
url = "http://127.0.0.1:18088/nifi-api/"
# process-groupsID，此处选取的fetchftp_group ID
fetchftp_groupid = "6b8030b7-7770-3602-8659-b3928c46f595"
api_summary = "flow/cluster/summary"
api_status = "flow/status"
api_about = "flow/about"
api_diagnostics = "system-diagnostics"
api_process_groups = "process-groups/"


class UrlFunction:

    def __init__(self, part):
        self.url_cluster = http.request('GET', url + part)
        # bytes转换为string
        self.string_url_cluster = str(self.url_cluster.data, encoding="UTF-8")
        # 字符串转换为字典
        self.dict_url_cluster = json.loads(self.string_url_cluster)


class ClusterSummary:
    def __init__(self):
        self.url_data = UrlFunction(api_summary).dict_url_cluster
        self.url_cluster = UrlFunction(api_summary).url_cluster

        # 连接的NIFI节点数量

    def connectedNodeCount(self):
        print(self.url_data["clusterSummary"]["connectedNodeCount"])

        # 总的节点数量

    def totalNodeCount(self):
        print(self.url_data["clusterSummary"]["totalNodeCount"])

    def nodeException(self):
        # 如果节点连接异常输出0
        con = self.url_data["clusterSummary"]["connectedNodeCount"]
        total = self.url_data["clusterSummary"]["totalNodeCount"]
        if con < total:
            print(1)
        else:
            print(0)
        self.url_cluster.close()


class ClusterStatus:
    def __init__(self):
        self.url_data = UrlFunction(api_status).dict_url_cluster
        self.url_cluster = UrlFunction(api_status).url_cluster

        # 活动的线程数量activeThreadCount

    def activeThreadCount(self):
        print(self.url_data["controllerStatus"]["activeThreadCount"])

        # 当前在Process Group中排队的FlowFiles数目 queued

    def queued(self):
        queue = self.url_data["controllerStatus"]["queued"]
        # 21,613 (16.68 GB) ,提取前面的数字
        queue_count = re.findall(r'\d+\S+\d+', queue)
        print('%.0f' % float(queue_count[0]))


class ClusterAbout:
    def __init__(self):
        self.url_data = UrlFunction(api_about).dict_url_cluster
        self.url_cluster = UrlFunction(api_about).url_cluster

        # NIFI当前版本

    def version(self):
        print(self.url_data["about"]["version"])

        # NIFI buildTag

    def buildTag(self):
        print(self.url_data["about"]["buildTag"])


class ClustersystemDiagnostics:
    def __init__(self):
        self.url_data = UrlFunction(api_diagnostics).dict_url_cluster
        self.url_cluster = UrlFunction(api_diagnostics).url_cluster
        self.systemDiagnostics = self.url_data["systemDiagnostics"]["aggregateSnapshot"]
        self.flowFileRepositoryStorageUsage = self.systemDiagnostics["flowFileRepositoryStorageUsage"]

        # 总的非堆内存

    """                                                                                                                             
    非堆就是JVM留给 自己用的，所以方法区、JVM内部处理或优化所需的内存                                                                                            
    每个类结构(如运行时常数池、字段和方法数据)以及方法和构造方法的代码都在非堆内存中                                                                                       
    """

    # zabbix 单位为GB
    def totalNonHeap(self):
        if 'GB' in self.systemDiagnostics["totalNonHeap"]:
            sub_queuedSize = re.findall(r'\d+\S\d*', self.systemDiagnostics["totalNonHeap"])
            print('%.2f' % float(sub_queuedSize[0]))
        if 'MB' in self.systemDiagnostics["totalNonHeap"]:
            sub_queuedSize = re.findall(r'\d+', self.systemDiagnostics["totalNonHeap"])
            sub_queuedSize = int(sub_queuedSize[0]) / 1000
            print('%.2f' % float(sub_queuedSize))

        # 已使用的非堆内存

    def usedNonHeap(self):
        if 'GB' in self.systemDiagnostics["usedNonHeap"]:
            sub_queuedSize = re.findall(r'\d+\S\d*', self.systemDiagnostics["usedNonHeap"])
            print('%.2f' % float(sub_queuedSize[0]))
        if 'MB' in self.systemDiagnostics["usedNonHeap"]:
            sub_queuedSize = re.findall(r'\d+', self.systemDiagnostics["usedNonHeap"])
            sub_queuedSize = int(sub_queuedSize[0]) / 1000
            print('%.2f' % float(sub_queuedSize))

        # 总的堆内存

    def totalHeap(self):
        if 'GB' in self.systemDiagnostics["totalHeap"]:
            sub_queuedSize = re.findall(r'\d+\S\d*', self.systemDiagnostics["totalHeap"])
            print('%.2f' % float(sub_queuedSize[0]))
        if 'MB' in self.systemDiagnostics["totalHeap"]:
            sub_queuedSize = re.findall(r'\d+', self.systemDiagnostics["totalHeap"])
            sub_queuedSize = int(sub_queuedSize[0]) / 1000
            print('%.2f' % float(sub_queuedSize))

        # 已使用的堆内存

    def usedHeap(self):
        if 'GB' in self.systemDiagnostics["usedHeap"]:
            sub_queuedSize = re.findall(r'\d+\S\d*', self.systemDiagnostics["usedHeap"])
            print('%.2f' % float(sub_queuedSize[0]))
        if 'MB' in self.systemDiagnostics["usedHeap"]:
            sub_queuedSize = re.findall(r'\d+', self.systemDiagnostics["usedHeap"])
            sub_queuedSize = int(sub_queuedSize[0]) / 1000
            print('%.2f' % float(sub_queuedSize))

        # 处理器负载

    def processorLoadAverage(self):
        print('%.2f' % float(self.systemDiagnostics["processorLoadAverage"]))

        # 总的线程数

    def totalThreads(self):
        print(self.systemDiagnostics["totalThreads"])

        # 总的守护线程数

    def daemonThreads(self):
        print(self.systemDiagnostics["daemonThreads"])

        # 流文件存储总使用空间

    # zabbix单位为TB
    def totalSpace(self):
        if 'TB' in self.flowFileRepositoryStorageUsage["totalSpace"]:
            sub_queuedSize = re.findall(r'\d+\S\d*', self.flowFileRepositoryStorageUsage["totalSpace"])
            print('%.2f' % float(sub_queuedSize[0]))
        if 'GB' in self.flowFileRepositoryStorageUsage["totalSpace"]:
            sub_queuedSize = re.findall(r'\d+', self.flowFileRepositoryStorageUsage["totalSpace"])
            sub_queuedSize = int(sub_queuedSize[0]) / 1000
            print('%.2f' % float(sub_queuedSize))

        #  流文件存储已使用空间     

    # zabbix单位为TB
    def usedSpace(self):
        if 'TB' in self.flowFileRepositoryStorageUsage["usedSpace"]:
            sub_queuedSize = re.findall(r'\d+\S\d*', self.flowFileRepositoryStorageUsage["usedSpace"])
            print('%.2f' % float(sub_queuedSize[0]))
        if 'GB' in self.flowFileRepositoryStorageUsage["usedSpace"]:
            sub_queuedSize = re.findall(r'\d+', self.flowFileRepositoryStorageUsage["usedSpace"])
            sub_queuedSize = int(sub_queuedSize[0]) / 1000
            print('%.2f' % float(sub_queuedSize))  


####################################################################
#                   ClusterSummary                                 #
####################################################################

if sys.argv[1] == "connectedNodeCount":
    ClusterSummary().connectedNodeCount()
elif sys.argv[1] == "totalNodeCount":
    ClusterSummary().totalNodeCount()
elif sys.argv[1] == "nodeException":
    ClusterSummary().nodeException()

####################################################################
#                     ClusterStatus                                #
####################################################################

elif sys.argv[1] == "activeThreadCount":
    ClusterStatus().activeThreadCount()
elif sys.argv[1] == "queued":
    ClusterStatus().queued()

####################################################################
#                     ClusterAbout                                 #
####################################################################

elif sys.argv[1] == "version":
    ClusterAbout().version()
elif sys.argv[1] == "buildTag":
    ClusterAbout().buildTag()

####################################################################
#                     ClustersystemDiagnostics                     #
####################################################################

elif sys.argv[1] == "totalNonHeap":
    ClustersystemDiagnostics().totalNonHeap()
elif sys.argv[1] == "usedNonHeap":
    ClustersystemDiagnostics().usedNonHeap()
elif sys.argv[1] == "totalHeap":
    ClustersystemDiagnostics().totalHeap()
elif sys.argv[1] == "usedHeap":
    ClustersystemDiagnostics().usedHeap()
elif sys.argv[1] == "processorLoadAverage":
    ClustersystemDiagnostics().processorLoadAverage()
elif sys.argv[1] == "totalThreads":
    ClustersystemDiagnostics().totalThreads()
elif sys.argv[1] == "daemonThreads":
    ClustersystemDiagnostics().daemonThreads()
elif sys.argv[1] == "totalSpace":
    ClustersystemDiagnostics().totalSpace()
elif sys.argv[1] == "usedSpace":
    ClustersystemDiagnostics().usedSpace()
