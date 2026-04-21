#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import csv
import os
import sys
import re
from datetime import datetime, timedelta

PROMETHEUS_HOST = "http://localhost:9090"
PROMETHEUS_API = f"{PROMETHEUS_HOST}/api/v1"

INSTANCE_TO_HOSTNAME = {
    "cpf-1:9100": "cpf-1",
    "cpf-2:9100": "cpf-2",
    "cpf-3:9100": "cpf-3",
    "cpf-4:9100": "cpf-4",
    "cpf-1:9402": "cpf-1",
    "cpf-2:9401": "cpf-2",
    "cpf-3:9401": "cpf-3",
    "cpf-4:9401": "cpf-4",
    "cpf-2:9404": "cpf-2",
    "cpf-3:9404": "cpf-3",
    "cpf-4:9404": "cpf-4",
    "cpf-1:9403": "cpf-1",
}

SERVICE_PORTS = {
    "9100": "node_exporter",
    "9401": "datanode",
    "9402": "namenode",
    "9403": "resourcemanager",
    "9404": "nodemanager"
}

def parse_time_to_s(time_str):
    """将时间字符串转换为秒时间戳"""
    try:
        dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
        return int(dt.timestamp())
    except ValueError:
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            return int(dt.timestamp())
        except ValueError:
            return int(datetime.now().timestamp())

def extract_hostname_from_instance(instance):
    """从instance标签提取hostname"""
    if not instance:
        return "unknown"
    
    if instance in INSTANCE_TO_HOSTNAME:
        return INSTANCE_TO_HOSTNAME[instance]
    
    if ":" in instance:
        hostname = instance.split(":")[0]
        if hostname.startswith("cpf-"):
            return hostname
    
    return instance.split(":")[0] if ":" in instance else instance

def extract_service_from_instance(instance):
    """从instance标签提取服务类型"""
    if not instance:
        return "unknown"
    
    if ":" in instance:
        port = instance.split(":")[1]
        return SERVICE_PORTS.get(port, f"port_{port}")
    
    return "unknown"

def get_metric_category(metric_name):
    """根据指标名称判断类别"""
    if "cpu" in metric_name or "load" in metric_name:
        return "cpu"
    elif "memory" in metric_name or "swap" in metric_name or "buffer" in metric_name or "cache" in metric_name:
        return "memory"
    elif "disk" in metric_name or "filesystem" in metric_name or "io_" in metric_name:
        return "disk"
    elif "network" in metric_name:
        return "network"
    else:
        return "other"

METRICS_CONFIG = {
    "cpu": [
        "rate(node_cpu_seconds_total{mode='user'}[1m])",
        "rate(node_cpu_seconds_total{mode='system'}[1m])",
        "rate(node_cpu_seconds_total{mode='idle'}[1m])",
        "rate(node_cpu_seconds_total{mode='iowait'}[1m])",
        "node_load1",
        "node_load5",
        "node_load15"
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
        "node_memory_Inactive_bytes"
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
        "node_filesystem_files"
    ],
    "network": [
        "node_network_receive_bytes_total",
        "node_network_transmit_bytes_total",
        "node_network_receive_errs_total",
        "node_network_transmit_errs_total",
        "node_network_receive_packets_total",
        "node_network_transmit_packets_total"
    ]
}

def query_metric_range(query, start_time, end_time, step=15):
    """查询指标的时间范围数据"""
    start_ts = parse_time_to_s(start_time)
    end_ts = parse_time_to_s(end_time)
    params = {
        "query": query,
        "start": start_ts,
        "end": end_ts,
        "step": f"{step}s"
    }
    try:
        response = requests.get(f"{PROMETHEUS_API}/query_range", params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("data", {}).get("result", [])
    except Exception as e:
        print(f"查询指标失败 ({query}): {e}")
        return []

def save_metrics_by_category(all_records, output_dir):
    """按节点和指标名称保存为CSV文件"""
    if not all_records:
        return []
    
    saved_files = []
    
    records_by_host_and_metric = {}
    for record in all_records:
        hostname = record.get("hostname", "unknown")
        metric = record.get("metric", "unknown")
        
        key = f"{hostname}/{metric}"
        if key not in records_by_host_and_metric:
            records_by_host_and_metric[key] = []
        records_by_host_and_metric[key].append(record)
    
    for key, records in sorted(records_by_host_and_metric.items()):
        if not records:
            continue
        
        hostname, metric = key.split("/")
        
        node_dir = os.path.join(output_dir, hostname)
        os.makedirs(node_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{metric}_{timestamp}.csv"
        filepath = os.path.join(node_dir, filename)
        
        fieldnames = ["timestamp", "timestamp_unix", "metric", "category", "hostname", "service", "device", "mode", "mountpoint", "value"]
        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
            writer.writeheader()
            writer.writerows(records)
        
        print(f"  ✓ {hostname}/{filename}: {len(records)} 条")
        saved_files.append(filepath)
    
    return saved_files

def collect_all_metrics(start_time, end_time, output_dir, step=15, categories=None):
    """收集所有指标"""
    if categories is None:
        categories = ["cpu", "memory"]
    
    print(f"\n{'='*60}")
    print(f"开始收集指标")
    print(f"时间范围: {start_time} - {end_time}")
    print(f"采集间隔: {step}秒")
    print(f"指标类别: {', '.join(categories)}")
    print(f"输出目录: {output_dir}")
    print(f"{'='*60}\n")
    
    all_records = []
    
    for category in categories:
        if category not in METRICS_CONFIG:
            print(f"警告: 未知指标类别 {category}")
            continue
        
        print(f"收集 {category} 指标...")
        metrics_list = METRICS_CONFIG[category]
        
        for metric_query in metrics_list:
            try:
                results = query_metric_range(metric_query, start_time, end_time, step)
                if results:
                    metric_name = metric_query.split("{")[0]
                    metric_category = get_metric_category(metric_name)
                    
                    for result in results:
                        metric_labels = result.get("metric", {})
                        instance = metric_labels.get("instance", "")
                        hostname = extract_hostname_from_instance(instance)
                        service = extract_service_from_instance(instance)
                        
                        if hostname == "unknown":
                            continue
                        
                        values = result.get("values", [])
                        
                        if not values:
                            if result.get("value"):
                                values = [result["value"]]
                        
                        for timestamp, value in values:
                            try:
                                dt = datetime.fromtimestamp(timestamp)
                                record = {
                                    "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                                    "timestamp_unix": timestamp,
                                    "metric": metric_name,
                                    "category": metric_category,
                                    "hostname": hostname,
                                    "service": service,
                                    "device": metric_labels.get("device", ""),
                                    "mode": metric_labels.get("mode", ""),
                                    "mountpoint": metric_labels.get("mountpoint", ""),
                                    "value": float(value)
                                }
                                all_records.append(record)
                            except Exception:
                                continue
                    
                    if values:
                        print(f"  ✓ {metric_name}: {len(values)} 条 ({hostname})")
            except Exception as e:
                print(f"  ✗ {metric_query}: {e}")
                continue
    
    print(f"\n按指标类别整理数据...")
    print(f"总共: {len(all_records)} 条记录\n")
    
    saved_files = save_metrics_by_category(all_records, output_dir)
    
    print(f"\n{'='*60}")
    print(f"指标收集完成")
    print(f"共保存 {len(saved_files)} 个文件")
    print(f"{'='*60}\n")
    
    return saved_files

def quick_collect_minutes(output_dir, minutes=10, step=15, categories=None):
    """快速收集最近N分钟的指标"""
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=minutes)
    
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    
    return collect_all_metrics(start_str, end_str, output_dir, step, categories)

def main():
    if len(sys.argv) < 4:
        print("用法: python collect_metrics.py <输出目录> <开始时间> <结束时间> [采集间隔秒]")
        print("时间格式: YYYY-MM-DDTHH:MM:SS")
        print("示例: python collect_metrics.py ./output 2024-01-01T00:00:00 2024-01-02T00:00:00 15")
        print("\n快速模式: python collect_metrics.py --quick <分钟数> <输出目录>")
        sys.exit(1)
    
    if sys.argv[1] == "--quick":
        minutes = int(sys.argv[2])
        output_dir = sys.argv[3]
        step = int(sys.argv[4]) if len(sys.argv) > 4 else 15
        quick_collect_minutes(output_dir, minutes, step)
    else:
        output_dir = sys.argv[1]
        start_time = sys.argv[2]
        end_time = sys.argv[3]
        step = int(sys.argv[4]) if len(sys.argv) > 4 else 15
        
        os.makedirs(output_dir, exist_ok=True)
        collect_all_metrics(start_time, end_time, output_dir, step)

if __name__ == "__main__":
    main()
