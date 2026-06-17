#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一配置模块 V2

集中管理所有配置，避免硬编码路径和重复配置
"""

import os

# 基础路径配置
SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_BASE = os.path.join(SCRIPTS_DIR, "collect_data/data")
LOG_DIR = os.path.join(SCRIPTS_DIR, "logs")

# 服务地址配置
PROMETHEUS_HOST = "http://localhost:9090"
PROMETHEUS_API = f"{PROMETHEUS_HOST}/api/v1"

LOKI_HOST = "http://localhost:3100"
LOKI_API = f"{LOKI_HOST}/loki/api/v1"

# HDFS配置
HDFS_NFS_MOUNT = "/hdfs-nfs"
HADOOP_HOME = os.environ.get("HADOOP_HOME", "/opt/hadoop")

# 集群拓扑配置
TOPOLOGY = {
    "master": "cxw-1",
    "slaves": ["cxw-2", "cxw-3", "cxw-4"],
    "all_nodes": ["cxw-1", "cxw-2", "cxw-3", "cxw-4"],
    "roles": {
        "cxw-1": ["namenode", "resourcemanager", "historyserver"],
        "cxw-2": ["datanode", "nodemanager"],
        "cxw-3": ["datanode", "nodemanager"],
        "cxw-4": ["datanode", "nodemanager"]
    },
    "services": {
        "namenode": "cxw-1:9404",
        "datanode": ["cxw-2:9405", "cxw-3:9405", "cxw-4:9405"],
        "resourcemanager": "cxw-1:9406",
        "nodemanager": ["cxw-2:9407", "cxw-3:9407", "cxw-4:9407"],
        "historyserver": "cxw-1"
    }
}

# 实例映射配置
INSTANCE_TO_HOSTNAME = {
    "cxw-1:9100": "cxw-1",
    "cxw-2:9100": "cxw-2",
    "cxw-3:9100": "cxw-3",
    "cxw-4:9100": "cxw-4",
    "cxw-1:9404": "cxw-1",
    "cxw-2:9405": "cxw-2",
    "cxw-3:9405": "cxw-3",
    "cxw-4:9405": "cxw-4",
    "cxw-1:9406": "cxw-1",
    "cxw-2:9407": "cxw-2",
    "cxw-3:9407": "cxw-3",
    "cxw-4:9407": "cxw-4",
    "cxw-1:9408": "cxw-1",
    "cxw-1:9409": "cxw-1",
}

SERVICE_PORTS = {
    "9100": "node_exporter",
    "9404": "namenode",
    "9405": "datanode",
    "9406": "resourcemanager",
    "9407": "nodemanager",
    "9408": "secondarynamenode",
    "9409": "jobhistoryserver"
}

IP_TO_HOSTNAME = {
    "10.10.0.82": "cxw-1",
    "10.10.0.124": "cxw-2",
    "10.10.2.188": "cxw-3",
    "10.10.0.92": "cxw-4"
}

# 故障类型配置
FAULT_CONFIG = {
    "wordcount": {
        "description": "通用WordCount基准任务",
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "affected_services": ["nodemanager"],
        "injection_method": "标准WordCount实现，作为基准对比",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "script_dir": "common_mapreduce",
        "category": "baseline"
    },
    "data_skew": {
        "description": "数据倾斜故障 - 分区倾斜(Key Skew)",
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "affected_services": ["nodemanager"],
        "injection_method": "在Mapper代码中80%概率输出相同key，导致单个Reducer处理大部分数据",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "category": "data_distribution"
    },
    "data_bloat": {
        "description": "数据膨胀故障 - Mapper输出膨胀",
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "affected_services": ["datanode", "nodemanager"],
        "injection_method": "在Mapper代码中生成多倍中间数据",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "category": "data_distribution"
    },
    "task_fail": {
        "description": "任务失败故障 - Mapper代码随机异常",
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "affected_services": ["nodemanager"],
        "injection_method": "在Mapper代码中随机抛出RuntimeException(20%概率)",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "category": "task_execution"
    },
    "long_tail": {
        "description": "长尾任务故障 - Mapper代码指定休眠",
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "affected_services": ["nodemanager"],
        "injection_method": "在Mapper代码中指定任务sleep 60秒",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "category": "task_execution"
    },
    "wait_time": {
        "description": "等待时间异常 - ResourceManager进程挂起",
        "affected_nodes": ["cxw-1"],
        "affected_services": ["resourcemanager"],
        "injection_method": "挂起ResourceManager进程(SIGSTOP) 120秒",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "category": "scheduling"
    },
    "runtime_delta": {
        "description": "运行时间异常 - MRAppMaster进程挂起",
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "affected_services": ["nodemanager"],
        "injection_method": "挂起MRAppMaster进程(SIGSTOP) 120秒",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "script_dir": "run_time",
        "category": "scheduling"
    },
    "exit_time": {
        "description": "退出时间异常 - NodeManager进程挂起",
        "affected_nodes": ["cxw-2", "cxw-3", "cxw-4"],
        "affected_services": ["nodemanager"],
        "injection_method": "挂起NodeManager进程(SIGSTOP) 120秒",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "category": "node_management"
    },
    "network_latency": {
        "description": "网络延迟故障 - 网络层tc注入",
        "affected_nodes": [],
        "affected_services": ["all"],
        "injection_method": "使用tc命令注入100ms网络延迟",
        "nodes_known": False,
        "inject_stage": "pre-run",
        "script_type": "py",
        "category": "network"
    },
    "log_level_change": {
        "description": "日志级别变更故障 - 将Hadoop组件日志级别从INFO改为DEBUG",
        "affected_nodes": ["cxw-2"],
        "affected_services": ["datanode", "namenode"],
        "injection_method": "通过Hadoop logLevel Servlet将组件日志级别从INFO改为DEBUG，导致日志量暴增",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "category": "log_anomaly"
    },
    "process_restart": {
        "description": "进程重启故障 - Kill并重启DataNode进程",
        "affected_nodes": ["cxw-2"],
        "affected_services": ["datanode"],
        "injection_method": "停止DataNode进程后延迟重启，模拟进程崩溃恢复",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "category": "node_management"
    },
    "heartbeat_timeout": {
        "description": "心跳超时故障 - 屏蔽DataNode心跳端口",
        "affected_nodes": ["cxw-2"],
        "affected_services": ["datanode"],
        "injection_method": "使用iptables屏蔽DataNode心跳端口，导致节点被标记为DEAD",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "category": "node_management"
    },
    "disk_error": {
        "description": "磁盘IO错误故障 - 使用ChaosBlade注入磁盘IO异常",
        "affected_nodes": ["cxw-2"],
        "affected_services": ["datanode", "nodemanager"],
        "injection_method": "使用ChaosBlade注入磁盘IO错误，模拟磁盘故障",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "category": "hardware"
    }
}

# 指标配置
METRICS_CONFIG = {
    "cpu": [
        "rate(node_cpu_seconds_total{mode='user'}[1m])",
        "rate(node_cpu_seconds_total{mode='system'}[1m])",
        "rate(node_cpu_seconds_total{mode='idle'}[1m])",
        "rate(node_cpu_seconds_total{mode='iowait'}[1m])",
        "rate(node_cpu_seconds_total{mode='steal'}[1m])",
        "node_load1",
        "node_load5",
        "node_load15",
        "rate(process_cpu_seconds_total[1m])"
    ],
    "memory": [
        "node_memory_MemTotal_bytes",
        "node_memory_MemAvailable_bytes",
        "node_memory_MemFree_bytes",
        "node_memory_Buffers_bytes",
        "node_memory_Cached_bytes",
        "node_memory_SwapFree_bytes",
        "node_memory_SwapTotal_bytes",
        "node_memory_SwapCached_bytes",
        "node_memory_Active_bytes",
        "node_memory_Inactive_bytes",
        "node_memory_Dirty_bytes",
        "node_memory_Writeback_bytes",
        "rate(node_vmstat_pgmajfault[1m])",
        "rate(node_vmstat_pgfault[1m])",
        "node_memory_MemUsed_percent",
        "node_memory_SwapUsed_percent"
    ],
    "disk": [
        "node_disk_io_time_seconds_total",
        "node_disk_read_bytes_total",
        "node_disk_written_bytes_total",
        "node_disk_reads_completed_total",
        "node_disk_writes_completed_total",
        "node_filesystem_avail_bytes",
        "node_filesystem_size_bytes",
        "node_filesystem_files_free",
        "node_filesystem_files",
        "rate(node_disk_io_time_weighted_seconds_total[1m])",
        "node_disk_read_time_seconds_total",
        "node_disk_write_time_seconds_total",
        "node_disk_io_now"
    ],
    "network": [
        "node_network_receive_bytes_total",
        "node_network_transmit_bytes_total",
        "node_network_receive_errs_total",
        "node_network_transmit_errs_total",
        "node_network_receive_packets_total",
        "node_network_transmit_packets_total",
        "node_network_receive_drop_total",
        "node_network_transmit_drop_total",
        "rate(node_network_receive_bytes_total[1m])",
        "rate(node_network_transmit_bytes_total[1m])",
        "node_network_tcp_established",
        "node_network_tcp_time_wait",
        "node_network_tcp_close_wait"
    ],
    "hadoop": [
        # NameNode指标
        "hadoop_namenode_capacitytotal",
        "hadoop_namenode_capacityused",
        "hadoop_namenode_capacityremaining",
        "hadoop_namenode_used",
        "hadoop_namenode_free",
        "hadoop_namenode_blockstotal",
        "hadoop_namenode_missingblocks",
        "hadoop_namenode_corruptblocks",
        "hadoop_namenode_underreplicatedblocks",
        "hadoop_namenode_pendingreplicationblocks",
        "hadoop_namenode_scheduledreplicationblocks",
        "hadoop_namenode_percentused",
        "hadoop_namenode_totalload",
        "hadoop_namenode_numlivedatanodes",
        "hadoop_namenode_numdeaddatanodes",
        "hadoop_namenode_numdecomlivedatanodes",
        "hadoop_namenode_numdecomdeaddatanodes",
        "hadoop_namenode_volumefailurestotal",
        # DataNode指标
        "hadoop_datanode_capacity",
        "hadoop_datanode_capacityused",
        "hadoop_datanode_capacityremaining",
        "hadoop_datanode_byteswritten",
        "hadoop_datanode_bytesread",
        "hadoop_datanode_blockscached",
        "hadoop_datanode_blocksgetlocalpathinfo",
        "hadoop_datanode_fsdatasetstate",
        "hadoop_datanode_numfailedvolumes",
        "hadoop_datanode_lastvolumefailuredate",
        "hadoop_datanode_estimatedcapacitylosttotal",
        # ResourceManager指标
        "hadoop_resourcemanager_numactivenms",
        "hadoop_resourcemanager_numlostnms",
        "hadoop_resourcemanager_numunhealthynms",
        "hadoop_resourcemanager_numdecommissionednms",
        "hadoop_resourcemanager_numrebootednms",
        "hadoop_resourcemanager_clustermetricsnumactiveapps",
        "hadoop_resourcemanager_clustermetricsnumpendingapps",
        "hadoop_resourcemanager_clustermetricsnumcompletedapps",
        "hadoop_resourcemanager_clustermetricsnumfailedapps",
        "hadoop_resourcemanager_clustermetricsnumkilledapps",
        "hadoop_resourcemanager_availablevcores",
        "hadoop_resourcemanager_allocatedvcores",
        "hadoop_resourcemanager_availablemb",
        "hadoop_resourcemanager_allocatedmb",
        "hadoop_resourcemanager_pendingmb",
        "hadoop_resourcemanager_pendingvcores",
        "hadoop_resourcemanager_reservedmb",
        "hadoop_resourcemanager_reservedvcores",
        # NodeManager指标
        "hadoop_nodemanager_containerlaunchernumops",
        "hadoop_nodemanager_containerlauncheravgtime",
        "hadoop_nodemanager_containerslaunched",
        "hadoop_nodemanager_containerscompleted",
        "hadoop_nodemanager_containersfailed",
        "hadoop_nodemanager_containerskilled",
        "hadoop_nodemanager_containersrunning",
        "hadoop_nodemanager_containersiniting",
        "hadoop_nodemanager_allocatedcontainers",
        "hadoop_nodemanager_availablecontainers",
        # MapReduce Job指标
        "hadoop_mapreduce_job_maptaskcount",
        "hadoop_mapreduce_job_reducetaskcount",
        "hadoop_mapreduce_job_failedmaptaskcount",
        "hadoop_mapreduce_job_failedreducetaskcount",
        "hadoop_mapreduce_job_killedmaptaskcount",
        "hadoop_mapreduce_job_killedreducetaskcount",
        "hadoop_mapreduce_job_completedmaptaskcount",
        "hadoop_mapreduce_job_completedreducetaskcount",
        "hadoop_mapreduce_job_mapinputrecords",
        "hadoop_mapreduce_job_mapoutputrecords",
        "hadoop_mapreduce_job_reduceinputrecords",
        "hadoop_mapreduce_job_reduceoutputrecords",
        "hadoop_mapreduce_job_mapinputbytes",
        "hadoop_mapreduce_job_mapoutputbytes",
        "hadoop_mapreduce_job_reduceshufflebytes"
    ],
    "jvm": [
        "jvm_memory_bytes_used{area='heap'}",
        "jvm_memory_bytes_used{area='nonheap'}",
        "jvm_memory_bytes_max{area='heap'}",
        "jvm_memory_bytes_max{area='nonheap'}",
        "jvm_memory_bytes_committed{area='heap'}",
        "jvm_memory_bytes_committed{area='nonheap'}",
        "jvm_gc_collection_seconds_sum{gc='G1 Young Generation'}",
        "jvm_gc_collection_seconds_sum{gc='G1 Old Generation'}",
        "jvm_gc_collection_seconds_count{gc='G1 Young Generation'}",
        "jvm_gc_collection_seconds_count{gc='G1 Old Generation'}",
        "rate(jvm_gc_collection_seconds_sum[1m])",
        "jvm_threads_current",
        "jvm_threads_daemon",
        "jvm_threads_peak",
        "jvm_threads_deadlocked",
        "process_open_fds",
        "process_max_fds",
        "jvm_classes_loaded",
        "jvm_classes_unloaded_total"
    ],
    "process": [
        "process_resident_memory_bytes",
        "process_virtual_memory_bytes",
        "process_cpu_seconds_total",
        "process_start_time_seconds",
        "process_uptime_seconds"
    ]
}

# 默认故障参数
DEFAULT_FAULT_PARAMS = {
    "minutes_before": 3,
    "minutes_after": 5,
    "wait_minutes": 1,
    "fault_duration": 120
}

# 核心指标配置 - 用于故障检测的关键指标
CORE_METRICS = {
    # JVM堆内存使用率 - 反映GC压力和内存问题
    "jvm_heap_used_ratio": "jvm_memory_bytes_used{area='heap'} / jvm_memory_bytes_max{area='heap'}",
    # 容器运行数量 - 反映NM状态和任务分布
    "container_running": "hadoop_nodemanager_ContainersRunning",
    # 容器失败数量 - 反映任务执行异常
    "container_failed": "hadoop_nodemanager_ContainersFailed",
    # CPU使用率 - 反映节点负载
    "cpu_usage": "rate(node_cpu_seconds_total[1m])",
    # 内存使用率 - 反映内存压力
    "memory_usage": "1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)"
}

# 调度器默认配置
DEFAULT_SCHEDULER_CONFIG = {
    "fault_types": ["data_skew", "data_bloat", "task_fail", "long_tail"],
    "include_normal": True,
    "fault_ratio": 0.7,
    "random_order": True,
    "ensure_all_faults": True,
    "interval_between_runs": 300,
    "interval_jitter": 60,
    "max_runs": None,
    "max_logs": None,
    "max_duration": None
}


# 辅助函数
def get_fault_script_path(fault_type):
    """获取故障注入脚本路径"""
    config = FAULT_CONFIG.get(fault_type, {})
    script_type = config.get("script_type", "sh")
    script_dir = config.get("script_dir", fault_type)
    return os.path.join(SCRIPTS_DIR, script_dir, f"inject_{fault_type}.{script_type}")


def get_fault_config(fault_type):
    """获取故障类型配置"""
    return FAULT_CONFIG.get(fault_type, {
        "description": "未知故障",
        "affected_nodes": [],
        "affected_services": ["nodemanager"],
        "injection_method": "未知",
        "nodes_known": False,
        "inject_stage": "dir-run",
        "script_type": "sh",
        "category": "unknown"
    })


def get_cluster_topology():
    """获取集群拓扑"""
    return TOPOLOGY


def get_all_nodes():
    """获取所有节点"""
    return TOPOLOGY["all_nodes"]


def get_master_node():
    """获取master节点"""
    return TOPOLOGY["master"]


def get_slave_nodes():
    """获取所有slave节点"""
    return TOPOLOGY["slaves"]


def is_fault_nodes_known(fault_type):
    """判断故障类型的受影响节点是否预先已知"""
    config = get_fault_config(fault_type)
    return config.get("nodes_known", False)


def get_fault_preset_nodes(fault_type):
    """获取故障预设的受影响节点"""
    config = get_fault_config(fault_type)
    return config.get("affected_nodes", [])


def get_metrics_for_fault(fault_type):
    """获取故障类型对应的指标类别"""
    mapping = {
        "wordcount": ["hadoop", "cpu", "memory"],
        "data_skew": ["hadoop", "jvm", "cpu"],
        "data_bloat": ["hadoop", "disk", "network"],
        "task_fail": ["hadoop", "jvm"],
        "long_tail": ["hadoop", "cpu", "jvm"],
        "wait_time": ["hadoop", "cpu", "jvm"],
        "runtime_delta": ["hadoop", "jvm", "cpu"],
        "exit_time": ["hadoop", "jvm"],
        "network_latency": ["network", "hadoop"]
    }
    return mapping.get(fault_type, ["cpu", "memory", "disk", "network", "hadoop", "jvm"])

# 新增故障类型配置
FAULT_CONFIG["disk_full"] = {
    "description": "磁盘空间耗尽故障 - 填充磁盘至90%导致写入失败",
    "affected_nodes": ["cxw-2"],
    "affected_services": ["datanode", "nodemanager"],
    "injection_method": "使用ChaosBlade disk fill填充磁盘至90%，模拟空间耗尽",
    "nodes_known": True,
    "inject_stage": "dir-run",
    "script_type": "py",
    "category": "hardware"
}

FAULT_CONFIG["network_loss"] = {
    "description": "网络丢包故障 - 30%丢包率导致TCP重传",
    "affected_nodes": ["cxw-2"],
    "affected_services": ["datanode", "nodemanager"],
    "injection_method": "使用ChaosBlade network loss注入30%丢包率",
    "nodes_known": True,
    "inject_stage": "dir-run",
    "script_type": "py",
    "category": "network"
}
