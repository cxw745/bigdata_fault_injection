#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
集群拓扑与特征向量模块 V3

用于故障检测和根因定位的特征向量生成
"""
import os
import json
from datetime import datetime

FAULT_LABELS = {
    "normal": 0,
    "wordcount": 0,
    "wait_time": 1,
    "exit_time": 2,
    "runtime_delta": 3,
    "data_skew": 4,
    "data_bloat": 5,
    "task_fail": 6,
    "long_tail": 7,
    "network_latency": 8
}

TOPOLOGY_FEATURES = {
    "version": "3.0",
    "cluster": {
        "name": "hadoop_cluster_4node",
        "total_nodes": 4,
        "master_nodes": 1,
        "slave_nodes": 3,
        "created_at": datetime.now().isoformat()
    },
    "nodes": {
        "cxw-1": {
            "hostname": "cxw-1",
            "ip": "10.10.0.82",
            "role": "master",
            "services": ["namenode", "resourcemanager", "historyserver"],
            "resources": {
                "cpu_cores": 32,
                "memory_gb": 128,
                "disk_gb": 2000
            },
            "ports": {
                "namenode": 9402,
                "resourcemanager": 9403,
                "prometheus": 9090,
                "loki": 3100
            },
            "connections": ["cxw-2", "cxw-3", "cxw-4"],
            "feature_vector_index": {
                "node_type_master": 1,
                "node_type_slave": 0,
                "has_namenode": 1,
                "has_resourcemanager": 1,
                "has_datanode": 0,
                "has_nodemanager": 0
            }
        },
        "cxw-2": {
            "hostname": "cxw-2",
            "ip": "10.10.0.124",
            "role": "slave",
            "services": ["datanode", "nodemanager"],
            "resources": {
                "cpu_cores": 32,
                "memory_gb": 128,
                "disk_gb": 4000
            },
            "ports": {
                "datanode": 9401,
                "nodemanager": 9404,
                "node_exporter": 9100
            },
            "connections": ["cxw-1"],
            "feature_vector_index": {
                "node_type_master": 0,
                "node_type_slave": 1,
                "has_namenode": 0,
                "has_resourcemanager": 0,
                "has_datanode": 1,
                "has_nodemanager": 1
            }
        },
        "cxw-3": {
            "hostname": "cxw-3",
            "ip": "10.10.2.188",
            "role": "slave",
            "services": ["datanode", "nodemanager"],
            "resources": {
                "cpu_cores": 32,
                "memory_gb": 128,
                "disk_gb": 4000
            },
            "ports": {
                "datanode": 9401,
                "nodemanager": 9404,
                "node_exporter": 9100
            },
            "connections": ["cxw-1"],
            "feature_vector_index": {
                "node_type_master": 0,
                "node_type_slave": 1,
                "has_namenode": 0,
                "has_resourcemanager": 0,
                "has_datanode": 1,
                "has_nodemanager": 1
            }
        },
        "cxw-4": {
            "hostname": "cxw-4",
            "ip": "10.10.0.92",
            "role": "slave",
            "services": ["datanode", "nodemanager"],
            "resources": {
                "cpu_cores": 32,
                "memory_gb": 128,
                "disk_gb": 4000
            },
            "ports": {
                "datanode": 9401,
                "nodemanager": 9404,
                "node_exporter": 9100
            },
            "connections": ["cxw-1"],
            "feature_vector_index": {
                "node_type_master": 0,
                "node_type_slave": 1,
                "has_namenode": 0,
                "has_resourcemanager": 0,
                "has_datanode": 1,
                "has_nodemanager": 1
            }
        }
    },
    "services": {
        "namenode": {
            "nodes": ["cxw-1"],
            "type": "master",
            "ha": False,
            "port": 9402
        },
        "resourcemanager": {
            "nodes": ["cxw-1"],
            "type": "master",
            "ha": False,
            "port": 9403
        },
        "historyserver": {
            "nodes": ["cxw-1"],
            "type": "master",
            "ha": False
        },
        "datanode": {
            "nodes": ["cxw-2", "cxw-3", "cxw-4"],
            "type": "slave",
            "ha": True,
            "port": 9401
        },
        "nodemanager": {
            "nodes": ["cxw-2", "cxw-3", "cxw-4"],
            "type": "slave",
            "ha": True,
            "port": 9404
        }
    },
    "network": {
        "topology": "rack_aware",
        "racks": {
            "rack_1": ["cxw-1", "cxw-3"],
            "rack_2": ["cxw-2"],
            "rack_3": ["cxw-4"]
        },
        "subnets": {
            "10.10.3.x": ["cxw-1", "cxw-3"],
            "10.10.1.x": ["cxw-2"],
            "10.10.0.x": ["cxw-4"]
        },
        "bandwidth_mbps": 1000
    },
    "feature_dimensions": [
        "node_type_master",
        "node_type_slave",
        "has_namenode",
        "has_resourcemanager",
        "has_datanode",
        "has_nodemanager",
        "cpu_usage",
        "memory_usage",
        "disk_io_read",
        "disk_io_write",
        "network_receive",
        "network_transmit",
        "jvm_heap_used",
        "jvm_gc_count",
        "container_running",
        "container_failed"
    ],
    "core_metrics": {
        "jvm_heap_used_ratio": {
            "description": "JVM堆内存使用率",
            "formula": "jvm_memory_bytes_used{area='heap'} / jvm_memory_bytes_max{area='heap'}",
            "category": "jvm"
        },
        "container_running": {
            "description": "运行中容器数量",
            "metric": "Hadoop_NodeManager_ContainersRunning",
            "category": "hadoop"
        },
        "container_failed": {
            "description": "失败容器数量",
            "metric": "Hadoop_NodeManager_ContainersFailed",
            "category": "hadoop"
        },
        "cpu_usage": {
            "description": "CPU使用率",
            "formula": "rate(node_cpu_seconds_total[1m])",
            "category": "cpu"
        },
        "memory_usage": {
            "description": "内存使用率",
            "formula": "1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)",
            "category": "memory"
        }
    }
}

FAULT_TO_SERVICE_MAPPING = {
    "normal": {
        "label": 0,
        "affected_services": [],
        "affected_nodes": [],
        "detection_metrics": [],
        "localization_features": [],
        "description": "正常任务，无故障"
    },
    "wordcount": {
        "label": 0,
        "affected_services": [],
        "affected_nodes": [],
        "detection_metrics": [],
        "localization_features": [],
        "description": "标准WordCount基准任务"
    },
    "wait_time": {
        "label": 1,
        "affected_services": ["resourcemanager"],
        "affected_nodes": ["cxw-1"],
        "injection_method": "挂起ResourceManager进程(SIGSTOP)",
        "detection_metrics": [
            "app_launch_delay",
            "scheduler_delay",
            "pending_containers"
        ],
        "localization_features": [
            "cpu_usage",
            "jvm_heap_used_ratio"
        ],
        "root_cause_indicators": {
            "resourcemanager_cpu_drop": "RM进程被挂起，CPU使用率接近0",
            "container_scheduling_delay": "容器调度延迟增加"
        },
        "description": "等待时间异常 - ResourceManager被挂起"
    },
    "exit_time": {
        "label": 2,
        "affected_services": ["nodemanager"],
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "injection_method": "挂起NodeManager进程(SIGSTOP)",
        "detection_metrics": [
            "nm_heartbeat_timeout",
            "container_exit_delay",
            "completed_containers"
        ],
        "localization_features": [
            "container_running",
            "container_failed"
        ],
        "root_cause_indicators": {
            "nodemanager_heartbeat_miss": "NM心跳丢失",
            "container_running_drop": "运行容器数下降",
            "specific_node_affected": "特定节点受影响"
        },
        "description": "退出时间异常 - NodeManager被挂起"
    },
    "runtime_delta": {
        "label": 3,
        "affected_services": ["mrappmaster"],
        "affected_nodes": ["运行任务的节点"],
        "injection_method": "挂起MRAppMaster进程(SIGSTOP)",
        "detection_metrics": [
            "am_heartbeat_interval",
            "task_duration"
        ],
        "localization_features": [
            "container_running",
            "jvm_gc_count"
        ],
        "root_cause_indicators": {
            "am_heartbeat_delay": "AM心跳延迟",
            "task_progress_stall": "任务进度停滞"
        },
        "description": "运行时间异常 - MRAppMaster被挂起"
    },
    "data_skew": {
        "label": 4,
        "affected_services": ["nodemanager"],
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "injection_method": "Mapper输出倾斜(80%数据输出相同key)",
        "detection_metrics": [
            "reduce_task_duration_max",
            "reduce_task_duration_avg",
            "shuffle_bytes_per_reducer"
        ],
        "localization_features": [
            "jvm_gc_count",
            "container_failed"
        ],
        "root_cause_indicators": {
            "reducer_duration_variance": "Reducer执行时间差异大",
            "single_reducer_hotspot": "单个Reducer成为热点"
        },
        "description": "数据倾斜 - 分区不均匀"
    },
    "data_bloat": {
        "label": 5,
        "affected_services": ["nodemanager", "datanode"],
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "injection_method": "Mapper输出膨胀(每条记录输出多份)",
        "detection_metrics": [
            "map_output_bytes",
            "shuffle_bytes_total",
            "disk_io_write"
        ],
        "localization_features": [
            "disk_io_write",
            "network_transmit"
        ],
        "root_cause_indicators": {
            "map_output_increase": "Map输出数据量异常增加",
            "shuffle_pressure": "Shuffle压力增大"
        },
        "description": "数据膨胀 - 中间数据膨胀"
    },
    "task_fail": {
        "label": 6,
        "affected_services": ["nodemanager"],
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "injection_method": "Mapper抛出异常(按task_id或ratio)",
        "detection_metrics": [
            "failed_map_tasks",
            "failed_reduce_tasks",
            "attempt_count"
        ],
        "localization_features": [
            "container_failed",
            "jvm_gc_count"
        ],
        "root_cause_indicators": {
            "container_failed_spike": "容器失败数突增",
            "task_retry_count": "任务重试次数增加"
        },
        "description": "任务失败 - 任务执行失败"
    },
    "long_tail": {
        "label": 7,
        "affected_services": ["nodemanager"],
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "injection_method": "Mapper延迟注入(按task_id)",
        "detection_metrics": [
            "map_task_duration_p95",
            "map_task_duration_p99",
            "task_duration_max"
        ],
        "localization_features": [
            "cpu_usage",
            "jvm_heap_used"
        ],
        "root_cause_indicators": {
            "task_duration_outlier": "部分任务执行时间异常长",
            "progress_skew": "进度不均衡"
        },
        "description": "长尾任务 - 部分任务延迟"
    },
    "network_latency": {
        "label": 8,
        "affected_services": ["all"],
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "injection_method": "tc命令注入网络延迟",
        "detection_metrics": [
            "shuffle_duration",
            "network_latency_avg",
            "tcp_retrans_rate"
        ],
        "localization_features": [
            "network_receive",
            "network_transmit"
        ],
        "root_cause_indicators": {
            "network_latency_increase": "网络延迟增加",
            "tcp_retransmission": "TCP重传率上升"
        },
        "description": "网络延迟 - 网络传输延迟"
    }
}

DEPENDENCY_GRAPH = {
    "mapreduce": {
        "depends_on": ["yarn", "hdfs"],
        "provides": ["batch_processing"]
    },
    "yarn": {
        "depends_on": ["hdfs"],
        "provides": ["resource_management"]
    },
    "hdfs": {
        "depends_on": [],
        "provides": ["storage"]
    },
    "resourcemanager": {
        "depends_on": ["namenode"],
        "affects": ["nodemanager"]
    },
    "nodemanager": {
        "depends_on": ["resourcemanager", "datanode"],
        "affects": []
    },
    "namenode": {
        "depends_on": [],
        "affects": ["datanode"]
    },
    "datanode": {
        "depends_on": ["namenode"],
        "affects": []
    }
}

FAULT_PROPAGATION_PATHS = {
    "wait_time": {
        "source": "cxw-1",
        "propagation": ["cxw-1 -> all_nodes"],
        "impact_scope": "cluster_wide",
        "description": "RM故障影响全局调度"
    },
    "exit_time": {
        "source": "single_slave_node",
        "propagation": ["affected_node -> running_containers"],
        "impact_scope": "node_local",
        "description": "NM故障影响单节点容器"
    },
    "runtime_delta": {
        "source": "application_master",
        "propagation": ["AM -> all_tasks_in_job"],
        "impact_scope": "job_local",
        "description": "AM故障影响单个任务"
    },
    "data_skew": {
        "source": "mapper_output",
        "propagation": ["mapper -> shuffle -> single_reducer"],
        "impact_scope": "task_local",
        "description": "数据倾斜影响单个Reducer"
    },
    "network_latency": {
        "source": "network_layer",
        "propagation": ["all_nodes <-> all_nodes"],
        "impact_scope": "cluster_wide",
        "description": "网络故障影响所有节点通信"
    }
}


def get_node_feature_vector(node_name):
    """获取指定节点的完整特征向量"""
    if node_name not in TOPOLOGY_FEATURES["nodes"]:
        return None

    node_info = TOPOLOGY_FEATURES["nodes"][node_name]
    feature_vector = {}

    for dim in TOPOLOGY_FEATURES["feature_dimensions"]:
        if dim in node_info.get("feature_vector_index", {}):
            feature_vector[dim] = node_info["feature_vector_index"][dim]
        else:
            feature_vector[dim] = 0.0

    return feature_vector


def get_all_nodes_feature_matrix():
    """获取所有节点的特征矩阵（用于批量计算）"""
    nodes = ["cxw-1", "cxw-2", "cxw-3", "cxw-4"]
    feature_matrix = []

    for node in nodes:
        fv = get_node_feature_vector(node)
        if fv:
            feature_matrix.append(list(fv.values()))

    return feature_matrix


def get_fault_detection_config(fault_type):
    """获取故障类型的检测配置"""
    return FAULT_TO_SERVICE_MAPPING.get(fault_type, {})


def get_fault_label(fault_type):
    """获取故障类型的标签"""
    return FAULT_LABELS.get(fault_type, -1)


def get_fault_by_label(label):
    """根据标签获取故障类型"""
    for fault_type, fault_label in FAULT_LABELS.items():
        if fault_label == label:
            return fault_type
    return None


def get_affected_nodes(fault_type):
    """获取故障影响的节点"""
    config = FAULT_TO_SERVICE_MAPPING.get(fault_type, {})
    return config.get("affected_nodes", [])


def get_root_cause_indicators(fault_type):
    """获取根因定位指标"""
    config = FAULT_TO_SERVICE_MAPPING.get(fault_type, {})
    return config.get("root_cause_indicators", {})


def generate_ml_training_data_template():
    """生成ML训练数据模板"""
    return {
        "feature_names": TOPOLOGY_FEATURES["feature_dimensions"],
        "feature_count": len(TOPOLOGY_FEATURES["feature_dimensions"]),
        "node_names": ["cxw-1", "cxw-2", "cxw-3", "cxw-4"],
        "node_count": 4,
        "fault_types": list(FAULT_LABELS.keys()),
        "normal_label": 0,
        "fault_labels": FAULT_LABELS,
        "vector_dimension": len(TOPOLOGY_FEATURES["feature_dimensions"]) * 4
    }


def save_topology_to_json(output_path=None):
    """保存拓扑数据为JSON文件"""
    data = {
        "version": "3.0",
        "generated_at": datetime.now().isoformat(),
        "topology": TOPOLOGY_FEATURES,
        "fault_mapping": FAULT_TO_SERVICE_MAPPING,
        "fault_labels": FAULT_LABELS,
        "dependency_graph": DEPENDENCY_GRAPH,
        "fault_propagation_paths": FAULT_PROPAGATION_PATHS,
        "ml_template": generate_ml_training_data_template()
    }

    if output_path is None:
        output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "topology_data.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"拓扑数据已保存到: {output_path}")
    return output_path


def load_topology_data():
    """加载拓扑数据"""
    default_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "topology_data.json")
    if os.path.exists(default_path):
        with open(default_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


if __name__ == "__main__":
    print("=" * 60)
    print("Hadoop集群拓扑数据 V3")
    print("=" * 60)

    print("\n【1. 集群拓扑】")
    print(f"  集群名称: {TOPOLOGY_FEATURES['cluster']['name']}")
    print(f"  总节点数: {TOPOLOGY_FEATURES['cluster']['total_nodes']}")
    print(f"  Master节点: {TOPOLOGY_FEATURES['cluster']['master_nodes']}")
    print(f"  Slave节点: {TOPOLOGY_FEATURES['cluster']['slave_nodes']}")

    print("\n【2. 节点信息】")
    for node, info in TOPOLOGY_FEATURES["nodes"].items():
        print(f"  {node} ({info['ip']}): {info['role']} - {', '.join(info['services'])}")

    print("\n【3. 特征维度】")
    for i, dim in enumerate(TOPOLOGY_FEATURES["feature_dimensions"]):
        print(f"  [{i:2d}] {dim}")

    print("\n【4. 故障类型标签映射】")
    for fault, label in sorted(FAULT_LABELS.items(), key=lambda x: x[1]):
        config = FAULT_TO_SERVICE_MAPPING.get(fault, {})
        desc = config.get("description", "")
        print(f"  [{label}] {fault}: {desc}")

    print("\n【5. 故障传播路径】")
    for fault, path in FAULT_PROPAGATION_PATHS.items():
        print(f"  {fault}:")
        print(f"    源: {path['source']}")
        print(f"    范围: {path['impact_scope']}")

    print("\n【6. 节点特征向量】")
    for node in ["cxw-1", "cxw-2", "cxw-3", "cxw-4"]:
        fv = get_node_feature_vector(node)
        print(f"  {node}: {fv}")

    print("\n【7. 特征矩阵（4节点 x 16维度）】")
    matrix = get_all_nodes_feature_matrix()
    for i, row in enumerate(matrix):
        print(f"  cxw-{i+1}: {row}")

    print("\n【8. ML训练数据模板】")
    ml_template = generate_ml_training_data_template()
    print(f"  特征数量: {ml_template['feature_count']}")
    print(f"  节点数量: {ml_template['node_count']}")
    print(f"  向量维度: {ml_template['vector_dimension']}")

    print("\n保存拓扑数据到JSON...")
    save_topology_to_json()
