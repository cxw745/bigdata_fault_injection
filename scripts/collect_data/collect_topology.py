#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
集群拓扑配置

固定网络拓扑:
- cpf-1: master节点 (NameNode, ResourceManager)
- cpf-2: slave节点 (DataNode, NodeManager)
- cpf-3: slave节点 (DataNode, NodeManager)
- cpf-4: slave节点 (DataNode, NodeManager)

故障类型分类:
- 已知节点: data_skew, data_bloat, task_fail (预设受影响节点)
- 随机节点: network_latency, cpu_load, memory_pressure (运行时动态确定)
"""

TOPOLOGY = {
    "master": "cpf-1",
    "slaves": ["cpf-2", "cpf-3", "cpf-4"],
    "all_nodes": ["cpf-1", "cpf-2", "cpf-3", "cpf-4"],
    "roles": {
        "cpf-1": ["namenode", "resourcemanager", "historyserver"],
        "cpf-2": ["datanode", "nodemanager"],
        "cpf-3": ["datanode", "nodemanager"],
        "cpf-4": ["datanode", "nodemanager"]
    },
    "services": {
        "namenode": "cpf-1:9402",
        "datanode": ["cpf-2:9401", "cpf-3:9401", "cpf-4:9401"],
        "resourcemanager": "cpf-1:9403",
        "nodemanager": ["cpf-2:9404", "cpf-3:9404", "cpf-4:9404"],
        "historyserver": "cpf-1"
    }
}

FAULT_CONFIG = {
    "data_skew": {
        "description": "数据倾斜故障",
        "affected_nodes": ["cpf-2", "cpf-3", "cpf-4"],
        "affected_services": ["nodemanager"],
        "injection_method": "在Mapper中注入热键",
        "nodes_known": True  # 已知节点
    },
    "data_bloat": {
        "description": "数据膨胀故障",
        "affected_nodes": ["cpf-2", "cpf-3", "cpf-4"],
        "affected_services": ["datanode", "nodemanager"],
        "injection_method": "生成大量中间数据",
        "nodes_known": True  # 已知节点
    },
    "task_fail": {
        "description": "任务失败故障",
        "affected_nodes": ["cpf-2", "cpf-3", "cpf-4"],
        "affected_services": ["nodemanager"],
        "injection_method": "随机杀死Map任务",
        "nodes_known": True  # 已知节点
    },
    "network_latency": {
        "description": "网络延迟故障",
        "affected_nodes": [],  # 运行时动态确定
        "affected_services": ["all"],
        "injection_method": "tc命令注入网络延迟",
        "nodes_known": False  # 随机节点
    },
    "cpu_load": {
        "description": "CPU高负载故障",
        "affected_nodes": [],  # 运行时动态确定
        "affected_services": ["nodemanager"],
        "injection_method": "启动CPU密集型进程",
        "nodes_known": False  # 随机节点
    },
    "memory_pressure": {
        "description": "内存压力故障",
        "affected_nodes": [],  # 运行时动态确定
        "affected_services": ["nodemanager"],
        "injection_method": "消耗节点内存",
        "nodes_known": False  # 随机节点
    }
}

def get_cluster_topology():
    """获取集群拓扑"""
    return TOPOLOGY

def get_fault_config(fault_type):
    """获取故障类型配置"""
    return FAULT_CONFIG.get(fault_type, {
        "description": "未知故障",
        "affected_nodes": [],
        "affected_services": ["nodemanager"],
        "injection_method": "未知",
        "nodes_known": False
    })

def is_fault_nodes_known(fault_type):
    """判断故障类型的受影响节点是否预先已知"""
    config = get_fault_config(fault_type)
    return config.get("nodes_known", False)

def get_fault_preset_nodes(fault_type):
    """获取故障预设的受影响节点（已知节点类型）"""
    config = get_fault_config(fault_type)
    return config.get("affected_nodes", [])

def get_master_node():
    """获取master节点"""
    return TOPOLOGY["master"]

def get_slave_nodes():
    """获取所有slave节点"""
    return TOPOLOGY["slaves"]

def get_all_nodes():
    """获取所有节点"""
    return TOPOLOGY["all_nodes"]

def get_node_roles(node):
    """获取节点角色"""
    return TOPOLOGY["roles"].get(node, [])

def get_service_nodes(service):
    """获取服务所在的节点"""
    service_info = TOPOLOGY["services"].get(service, [])
    if isinstance(service_info, list):
        return [s.split(":")[0] for s in service_info]
    elif isinstance(service_info, str):
        return [service_info.split(":")[0]]
    return []

def print_topology():
    """打印集群拓扑"""
    print("=" * 60)
    print("集群拓扑配置")
    print("=" * 60)
    print(f"\nMaster节点: {TOPOLOGY['master']}")
    print(f"Slave节点: {', '.join(TOPOLOGY['slaves'])}")
    print(f"所有节点: {', '.join(TOPOLOGY['all_nodes'])}")
    
    print("\n节点角色:")
    for node, roles in TOPOLOGY["roles"].items():
        print(f"  {node}: {', '.join(roles)}")
    
    print("\n故障类型:")
    print("\n已知节点类型:")
    for fault_type, info in FAULT_CONFIG.items():
        if info.get("nodes_known", False):
            print(f"  ✓ {fault_type}: {info['description']}")
            print(f"    影响节点: {', '.join(info['affected_nodes'])}")
    
    print("\n随机节点类型:")
    for fault_type, info in FAULT_CONFIG.items():
        if not info.get("nodes_known", False):
            print(f"  ? {fault_type}: {info['description']}")
            print(f"    影响节点: 运行时动态确定")
    
    print("=" * 60)

if __name__ == "__main__":
    print_topology()
