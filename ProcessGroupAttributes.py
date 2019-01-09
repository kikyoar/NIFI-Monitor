#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

@author: kikyoar
@contact: nokikyoar@gmail.com
@time: 2018/12/28 14:07

"""

import sys
import urllib3
import json
import re


http = urllib3.PoolManager()
# NIFI API
url = "http://127.0.0.1:18088/nifi-api/"
# process-groupsID，此处选取的fetchftp_group ID
# fetchftp_groupid = "6b8030b7-7770-3602-8659-b3928c46f595"
api_summary = "flow/cluster/summary"
api_status = "flow/status"
api_about = "flow/about"
api_diagnostics = "system-diagnostics"
api_process_groups = "process-groups/"


class ClusterProcessGroupsStaus:
    def __init__(self, fetchftp_groupid):
        """
            ClusterProcessGroups整体状态
        """

        self.url_cluster = http.request('GET', url + api_process_groups + fetchftp_groupid)
        # bytes转换为string
        self.string_url_cluster = str(self.url_cluster.data, encoding="UTF-8")
        # 字符串转换为字典
        self.dict_url_cluster = json.loads(self.string_url_cluster)
        # process group的status字典列表
        self.dict_temp = self.dict_url_cluster["status"]
        # 取出status字典中的aggregateSnapshot字典列表
        self.aggregateSnapshot = self.dict_temp['aggregateSnapshot']

        """                                                                                                           
            ClusterProcessGroups连接器整体状态                                                                               
        """
        self.url_cluster_connections = http.request('GET', url + api_process_groups + fetchftp_groupid + "/connections")
        # bytes转换为string
        self.string_url_cluster_connections = str(self.url_cluster_connections.data, encoding="UTF-8")
        # 字符串转换为字典
        self.dict_url_cluster_connections = json.loads(self.string_url_cluster_connections)
        # 取出connections字典列表
        self.list_connections = self.dict_url_cluster_connections["connections"]

        """                                                                                                               
            ClusterProcessGroups整体状态                                                                                      
        """

        # processGroup报错信息

    def errorMessages(self):
        list_temp = self.dict_url_cluster["bulletins"]
        if len(list_temp):  # 判断列表是否存在
            for dict_temp in list_temp:
                if 'bulletin' in dict_temp:  # 判断是否存在bulletin这个字段的字典
                    dict_message = (dict_temp.get("bulletin"))  # 得到新的字典
                    if 'ERROR' in dict_message.values():  # 判断level是否有'error'错误
                        print(dict_message.get('nodeAddress') + ": " + dict_message.get('message'))  # 输出error message

    # flowFilesIn个数
    def flowFilesIn(self):
        print(self.aggregateSnapshot['flowFilesIn'])

        # flowFiles排队数

    def flowFilesQueued(self):
        print(self.aggregateSnapshot['flowFilesQueued'])

        #  处理器从磁盘读取的FlowFile内容的总大小

    # def read(self):
    #     print(self.aggregateSnapshot['read'])
    #
    #     # 处理器从写入磁盘的FlowFile内容的总大小
    #
    # def written(self):
    #     print(self.aggregateSnapshot['written'])
    #
    # 文件队列大小

    # 由于有TB、MB和GB、KB、bytes的存在，所以分开判断，bytes数据量太小，忽略不计
    def queuedSize(self):
        if 'GB' in self.aggregateSnapshot['queuedSize']:
            sub_queuedSize = re.findall(r'\d+\S\d*', self.aggregateSnapshot['queuedSize'])
            print('%.2f' % float(sub_queuedSize[0]))
        elif 'bytes' in self.aggregateSnapshot['queuedSize']:
            print(0)
        elif 'TB' in self.aggregateSnapshot['queuedSize']:
            sub_queuedSize = re.findall(r'\d+', self.aggregateSnapshot['queuedSize'])
            sub_queuedSize = int(sub_queuedSize[0]) * 1000
            print('%.2f' % float(sub_queuedSize))
        elif 'MB' in self.aggregateSnapshot['queuedSize']:
            sub_queuedSize = re.findall(r'\d+', self.aggregateSnapshot['queuedSize'])
            sub_queuedSize = int(sub_queuedSize[0]) / 1000
            print('%.2f' % float(sub_queuedSize))
        elif 'KB' in self.aggregateSnapshot['queuedSize']:
            print(0)

        # flowFilesOut个数

    def flowFilesOut(self):
        print(self.aggregateSnapshot['flowFilesOut'])

    """                                                                                                               
        ClusterProcessGroups连接器整体状态                                                                                   
    """

    def connections(self):
        for connection in self.list_connections:
            # print(connection)
            """                                                                                                       
            结构是：                                                                                                      
            "connections": [{                                                                                         
                "revision": { ... },                                                                                  
                "bulletins": [],                                                                                      
                "component": {                                                                                        
                    "source":{...},                                                                                   
                    "destination": {...},                                                                             
                     ....                                                                                             
                    },                                                                                                
                "status": { ... }                                                                                     
            """
            component = connection.get('component')
            status = connection.get('status')
            sourcename = (component.get("source")).get("name")
            destinationname = (component.get("destination")).get("name")
            aggregateSnapshot = status.get("aggregateSnapshot")
            backPressureObjectThreshold = component.get("backPressureObjectThreshold")
            backPressureDataSizeThreshold = component.get("backPressureDataSizeThreshold")
            queuedSize = aggregateSnapshot.get("queuedSize")
            queuedCount = aggregateSnapshot.get("queuedCount")
            backPressureDataSize = (re.findall(r'\d+', backPressureDataSizeThreshold))[0]
            sub_queuedCount = int(queuedCount) / int(backPressureObjectThreshold)

            # 由于有MB和GB、bytes的存在，所以分开判断，bytes数据量太小，忽略不计
            if 'MB' in queuedSize:
                sub_queuedSize = re.findall(r'\d+', queuedSize)
                # 判断是否为空列表
                if len(sub_queuedSize) > 0:
                    sub_conversion = int(sub_queuedSize[0]) / 1000
                    conversion = int(sub_conversion) / int(backPressureDataSize)
                    # 0.61-0.85为黄色   0.85以上为红色
                    if 0.61 < conversion < 0.85:
                        print(sourcename + " to " + destinationname + " 数据存在拥挤情况，请检查，拥挤率为: " + str(conversion))
                    elif conversion >= 0.85:
                        print(sourcename + " to " + destinationname + " 数据道路阻塞，数据通行受阻,请疏通，阻塞率为: " + str(conversion))

            if 'GB' in queuedSize:
                sub_queuedSize = re.findall(r'\d+\S\d+', queuedSize)
                #判断是否为空列表
                if len(sub_queuedSize) > 0:
                    sub_conversion = sub_queuedSize[0]
                    conversion = float(sub_conversion) / int(backPressureDataSize)
                    if 0.61 < conversion < 0.85:
                        print(sourcename + " to " + destinationname + " 数据存在拥挤情况，请检查，拥挤率为: " + str(conversion))
                    elif conversion >= 0.85:
                        print(sourcename + " to " + destinationname + " 数据阻塞，数据通行受阻，请疏通，阻塞率为: " + str(conversion))
            # 输出的是queue的告警
            if 0.61 < sub_queuedCount < 0.85:
                print(sourcename + " to " + destinationname + " 队列存在拥挤情况，请检查，拥挤率为: " + str(sub_queuedCount))
            elif sub_queuedCount >= 0.85:
                print(sourcename + " to " + destinationname + " 队列阻塞，数据通行受阻，请疏通，阻塞率为: " + str(sub_queuedCount))


if sys.argv[2] == "errorMessages":
    ClusterProcessGroupsStaus(sys.argv[1]).errorMessages()
elif sys.argv[2] == "flowFilesIn":
    ClusterProcessGroupsStaus(sys.argv[1]).flowFilesIn()
elif sys.argv[2] == "flowFilesQueued":
    ClusterProcessGroupsStaus(sys.argv[1]).flowFilesQueued()
# elif sys.argv[2] == "read":
#     ClusterProcessGroupsStaus(sys.argv[1]).read()
# elif sys.argv[2] == "written":
#     ClusterProcessGroupsStaus(sys.argv[1]).written()
elif sys.argv[2] == "queuedSize":
    ClusterProcessGroupsStaus(sys.argv[1]).queuedSize()
elif sys.argv[2] == "flowFilesOut":
    ClusterProcessGroupsStaus(sys.argv[1]).flowFilesOut()
elif sys.argv[2] == "connections":
    ClusterProcessGroupsStaus(sys.argv[1]).connections()
