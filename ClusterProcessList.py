#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

@author: kikyoar
@contact: nokikyoar@gmail.com
@time: 2018/12/27 14:07

"""

import urllib3
import json

http = urllib3.PoolManager()
# NIFI API
url = "http://127.0.0.1:18088/nifi-api/"
api_summary = "flow/cluster/summary"
api_status = "flow/status"
api_about = "flow/about"
api_diagnostics = "system-diagnostics"
api_process_groups = "process-groups/"
api_process_groups_all = "flow/process-groups/root/status?recursive=true"


class ClusterProcessGroupsAll:
    def __init__(self):
        """
            ClusterProcessGroups整体状态
        """
        self.url_cluster_all = http.request('GET', url + api_process_groups_all)
        # bytes转换为string
        self.string_url_cluster_all = str(self.url_cluster_all.data, encoding="UTF-8")
        # 字符串转换为字典
        self.dict_url_cluster_all = json.loads(self.string_url_cluster_all)
        # 获取所有的"processGroupStatus"
        self.dict_url_processGroupStatus_all = self.dict_url_cluster_all.get("processGroupStatus")
        # 获取所有的"aggregateSnapshot"
        self.dict_url_aggregateSnapshot_all = self.dict_url_processGroupStatus_all.get("aggregateSnapshot")
        # 获取所有的 "processGroupStatusSnapshots"
        self.dict_processgroup_list = self.dict_url_aggregateSnapshot_all.get("processGroupStatusSnapshots")


# 提取ID和NAME，ID是为了给ProcessGroupAttributes.py传参
# 提取NAME的目的是为了方便在ZABBIX上呈现

def processgroup_list():
    processgroup_list = []
    processgroup_dict = {"data": None}

    for process_id in ClusterProcessGroupsAll().dict_processgroup_list:
        # 获取每一个组的ID
        fetchftp_groupid = process_id.get('id')
        # 获取每一个组的名字
        processgroup_name = (process_id.get('processGroupStatusSnapshot')).get('name')

        pdict = {}
        pdict["{#PROCESS_GROUPID}"] = fetchftp_groupid
        pdict["{#PROCESS_GROUPNAME}"] = processgroup_name
        processgroup_list.append(pdict)

    processgroup_dict["data"] = processgroup_list
    jsonStr = json.dumps(processgroup_dict, sort_keys=True, indent=4)

    return jsonStr


print(processgroup_list())
