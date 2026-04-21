# 拓扑数据构建指南 V3

## 一、当前系统拓扑

### 1. 物理拓扑

```
┌─────────────────────────────────────────────────────────────┐
│                         集群拓扑                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   ┌─────────────┐                                          │
│   │   cpf-1     │  Master节点                               │
│   │  (10.10.3.183)                                          │
│   │             │  - NameNode (HDFS)                        │
│   │             │  - ResourceManager (YARN)                 │
│   │             │  - HistoryServer                          │
│   └──────┬──────┘                                          │
│          │                                                  │
│          │  网络连接                                         │
│          │                                                  │
│   ┌──────┴──────┬──────────────┬──────────────┐            │
│   │             │              │              │             │
│   ▼             ▼              ▼              ▼             │
│ ┌──────┐   ┌──────┐     ┌──────┐     ┌──────┐             │
│ │cpf-2 │   │cpf-3 │     │cpf-4 │                        │
│ │(10.10│   │(10.10│     │(10.10│                        │
│ │.1.96)│   │.3.222│     │.0.176│                        │
│ │      │   │      │     │      │                        │
│ │DataNode│  │DataNode│   │DataNode│                       │
│ │NodeManager│ │NodeManager│ │NodeManager│                      │
│ └──────┘   └──────┘     └──────┘                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 2. 服务分布

| 节点 | 角色 | 服务 | 端口 |
|------|------|------|------|
| cpf-1 | Master | NameNode | 9402 |
| cpf-1 | Master | ResourceManager | 9403 |
| cpf-1 | Master | HistoryServer | - |
| cpf-2 | Slave | DataNode | 9401 |
| cpf-2 | Slave | NodeManager | 9404 |
| cpf-3 | Slave | DataNode | 9401 |
| cpf-3 | Slave | NodeManager | 9404 |
| cpf-4 | Slave | DataNode | 9401 |
| cpf-4 | Slave | NodeManager | 9404 |

### 3. 监控部署

| 组件 | 部署位置 | 端口 | 用途 |
|------|----------|------|------|
| Prometheus | cpf-1 | 9090 | 指标收集 |
| Loki | cpf-1 | 3100 | 日志收集 |
| Node Exporter | 所有节点 | 9100 | 系统指标 |
| Hadoop JMX Exporter | 所有节点 | 9401-9404 | Hadoop指标 |

---

## 二、拓扑数据构建选择

### 选择1：基于节点角色的拓扑

```python
# 节点角色拓扑
TOPOLOGY_ROLE = {
    "masters": ["cpf-1"],
    "slaves": ["cpf-2", "cpf-3", "cpf-4"],
    "all_nodes": ["cpf-1", "cpf-2", "cpf-3", "cpf-4"]
}

# 适用场景：
# - 区分Master/Slave故障影响
# - 分析单点故障vs分布式故障
# - 故障注入时选择目标节点
```

### 选择2：基于服务类型的拓扑

```python
# 服务类型拓扑
TOPOLOGY_SERVICE = {
    "hdfs": {
        "namenode": ["cpf-1"],
        "datanode": ["cpf-2", "cpf-3", "cpf-4"]
    },
    "yarn": {
        "resourcemanager": ["cpf-1"],
        "nodemanager": ["cpf-2", "cpf-3", "cpf-4"]
    }
}

# 适用场景：
# - 按服务类型分析故障传播
# - 服务级别的根因分析
# - 跨服务依赖分析
```

### 选择3：基于网络位置的拓扑

```python
# 网络拓扑（基于IP段）
TOPOLOGY_NETWORK = {
    "subnet_10.10.1": ["cpf-2"],      # 10.10.1.0/24
    "subnet_10.10.3": ["cpf-1", "cpf-3"],  # 10.10.3.0/24
    "subnet_10.10.0": ["cpf-4"]       # 10.10.0.0/24
}

# 适用场景：
# - 网络分区故障分析
# - 跨子网延迟/丢包影响
# - 网络拓扑感知的故障定位
```

### 选择4：基于故障传播路径的拓扑

```python
# 故障传播拓扑（有向图）
TOPOLOGY_PROPAGATION = {
    "cpf-1": ["cpf-2", "cpf-3", "cpf-4"],  # Master影响所有Slave
    "cpf-2": [],  # Slave故障不传播
    "cpf-3": [],
    "cpf-4": []
}

# 适用场景：
# - 故障传播建模
# - 级联故障分析
# - 影响范围预测
```

### 选择5：基于资源依赖的拓扑

```python
# 资源依赖拓扑
TOPOLOGY_DEPENDENCY = {
    "compute": {
        "nodes": ["cpf-2", "cpf-3", "cpf-4"],
        "depends_on": ["storage", "network"]
    },
    "storage": {
        "nodes": ["cpf-1", "cpf-2", "cpf-3", "cpf-4"],
        "depends_on": ["network"]
    },
    "network": {
        "nodes": ["cpf-1", "cpf-2", "cpf-3", "cpf-4"],
        "depends_on": []
    }
}

# 适用场景：
# - 资源瓶颈分析
# - 依赖故障传播
# - 服务降级策略
```

---

## 三、推荐的拓扑数据格式

### 综合拓扑（推荐）

```python
CLUSTER_TOPOLOGY = {
    # 基础信息
    "cluster_name": "cpf-cluster",
    "version": "1.0",
    
    # 节点信息
    "nodes": {
        "cpf-1": {
            "ip": "10.10.3.183",
            "role": "master",
            "services": ["namenode", "resourcemanager", "historyserver"],
            "resources": {"cpu": 32, "memory_gb": 128, "disk_gb": 2000},
            "ports": {"namenode": 9402, "resourcemanager": 9403}
        },
        "cpf-2": {
            "ip": "10.10.1.96",
            "role": "slave",
            "services": ["datanode", "nodemanager"],
            "resources": {"cpu": 32, "memory_gb": 128, "disk_gb": 4000},
            "ports": {"datanode": 9401, "nodemanager": 9404}
        },
        "cpf-3": {
            "ip": "10.10.3.222",
            "role": "slave",
            "services": ["datanode", "nodemanager"],
            "resources": {"cpu": 32, "memory_gb": 128, "disk_gb": 4000},
            "ports": {"datanode": 9401, "nodemanager": 9404}
        },
        "cpf-4": {
            "ip": "10.10.0.176",
            "role": "slave",
            "services": ["datanode", "nodemanager"],
            "resources": {"cpu": 32, "memory_gb": 128, "disk_gb": 4000},
            "ports": {"datanode": 9401, "nodemanager": 9404}
        }
    },
    
    # 服务分布
    "services": {
        "namenode": {"nodes": ["cpf-1"], "type": "master", "ha": False},
        "resourcemanager": {"nodes": ["cpf-1"], "type": "master", "ha": False},
        "datanode": {"nodes": ["cpf-2", "cpf-3", "cpf-4"], "type": "slave", "ha": True},
        "nodemanager": {"nodes": ["cpf-2", "cpf-3", "cpf-4"], "type": "slave", "ha": True}
    },
    
    # 网络拓扑
    "network": {
        "topology": "rack_aware",  # 机架感知
        "racks": {
            "rack_1": ["cpf-1", "cpf-3"],  # 10.10.3.x
            "rack_2": ["cpf-2"],            # 10.10.1.x
            "rack_3": ["cpf-4"]             # 10.10.0.x
        },
        "bandwidth_mbps": 1000
    },
    
    # 故障域
    "failure_domains": {
        "domain_1": ["cpf-1"],           # Master单点
        "domain_2": ["cpf-2", "cpf-3"],  # 同网段
        "domain_3": ["cpf-4"]            # 不同网段
    },
    
    # 依赖关系
    "dependencies": {
        "mapreduce": ["yarn", "hdfs"],
        "yarn": ["hdfs"],
        "hdfs": []
    }
}
```

---

## 四、拓扑数据应用场景

### 1. 故障注入位置选择

```python
def select_fault_target(fault_type: str, topology: dict) -> list:
    """
    根据故障类型选择目标节点
    """
    if fault_type in ["namenode_slow", "wait_time"]:
        # 影响Master的故障
        return topology["services"]["namenode"]["nodes"]
    elif fault_type in ["data_skew", "long_tail", "task_fail"]:
        # 影响Slave的故障
        return topology["services"]["nodemanager"]["nodes"]
    else:
        # 全网故障
        return topology["nodes"].keys()
```

### 2. 故障传播分析

```python
def analyze_fault_propagation(fault_node: str, topology: dict) -> dict:
    """
    分析故障传播范围
    """
    affected = {
        "direct": [],      # 直接影响
        "indirect": [],    # 间接影响
        "services": []     # 受影响服务
    }
    
    node_info = topology["nodes"][fault_node]
    
    # 直接影响：同服务其他节点
    for service in node_info["services"]:
        service_nodes = topology["services"][service]["nodes"]
        affected["direct"].extend(service_nodes)
    
    # 间接影响：依赖该节点的服务
    for svc, deps in topology["dependencies"].items():
        if any(s in node_info["services"] for s in deps):
            affected["indirect"].extend(topology["services"][svc]["nodes"])
    
    return affected
```

### 3. 指标关联分析

```python
def correlate_metrics_by_topology(metrics: dict, topology: dict) -> dict:
    """
    基于拓扑关联指标
    """
    correlated = {
        "by_node": {},
        "by_service": {},
        "by_rack": {}
    }
    
    # 按节点分组
    for node in topology["nodes"]:
        correlated["by_node"][node] = filter_metrics_by_node(metrics, node)
    
    # 按服务分组
    for service in topology["services"]:
        nodes = topology["services"][service]["nodes"]
        correlated["by_service"][service] = aggregate_metrics(metrics, nodes)
    
    # 按机架分组
    for rack, nodes in topology["network"]["racks"].items():
        correlated["by_rack"][rack] = aggregate_metrics(metrics, nodes)
    
    return correlated
```

---

## 五、建议的拓扑数据收集

### 需要收集的拓扑相关指标

| 类别 | 指标 | 用途 |
|------|------|------|
| 网络 | 节点间延迟 | 网络分区检测 |
| 网络 | 跨机架流量 | 数据本地性分析 |
| 资源 | 节点CPU/内存使用率 | 资源瓶颈定位 |
| 资源 | 磁盘IO分布 | 存储热点检测 |
| 服务 | 服务心跳延迟 | 服务健康度 |
| 任务 | 任务分布 | 负载均衡分析 |

### 拓扑可视化建议

1. **实时监控拓扑图**：显示节点状态、服务健康度
2. **故障传播图**：动态展示故障影响范围
3. **热力图**：资源使用率、任务分布
4. **依赖图**：服务间调用关系
