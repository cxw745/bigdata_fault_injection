#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import csv
import os
import sys
import re
from datetime import datetime, timedelta
from collections import defaultdict

LOKI_HOST = "http://localhost:3100"
LOKI_API = f"{LOKI_HOST}/loki/api/v1"
HDFS_NFS_MOUNT = "/hdfs-nfs"

IP_TO_HOSTNAME = {
    "10.10.3.183": "cpf-1",
    "10.10.1.96": "cpf-2",
    "10.10.3.222": "cpf-3",
    "10.10.0.176": "cpf-4"
}

def parse_time_to_ns(time_str):
    """将时间字符串转换为纳秒时间戳"""
    try:
        dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
        return int(dt.timestamp() * 1e9)
    except ValueError:
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            return int(dt.timestamp() * 1e9)
        except ValueError:
            return int(datetime.now().timestamp() * 1e9)

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

def extract_node_from_filename(filename):
    """从filename路径提取节点名"""
    if not filename:
        return "unknown"
    
    patterns = {
        "datanode": r"datanode-(cpf-\d+)",
        "namenode": r"namenode-(cpf-\d+)",
        "nodemanager": r"nodemanager-(cpf-\d+)",
        "resourcemanager": r"resourcemanager-(cpf-\d+)",
        "secondarynamenode": r"secondarynamenode-(cpf-\d+)",
        "mapreduce_task": r"cpf-(\d+)_(\d+)"
    }
    
    for pattern_name, pattern in patterns.items():
        match = re.search(pattern, filename)
        if match:
            if pattern_name == "mapreduce_task":
                return f"cpf-{match.group(1)}"
            else:
                return match.group(1)
    
    return "unknown"

def extract_task_id_from_filename(filename):
    """从filename路径提取任务ID"""
    if not filename:
        return None
    
    match = re.search(r"application_(\d+_\d+)", filename)
    if match:
        return match.group(1)
    
    return None

def extract_component_from_filename(filename):
    """从filename提取组件名"""
    if not filename:
        return "unknown"
    
    if "datanode" in filename:
        return "datanode"
    elif "namenode" in filename:
        if "secondary" in filename:
            return "secondarynamenode"
        return "namenode"
    elif "nodemanager" in filename:
        return "nodemanager"
    elif "resourcemanager" in filename:
        return "resourcemanager"
    else:
        return "mapreduce"

def extract_level_from_line(line):
    """从日志行提取日志级别"""
    if not line:
        return "unknown"
    
    line_upper = line.upper()
    
    if "FATAL" in line_upper or "ERROR" in line_upper:
        return "ERROR"
    elif "WARN" in line_upper:
        return "WARN"
    elif "INFO" in line_upper:
        return "INFO"
    elif "DEBUG" in line_upper:
        return "DEBUG"
    elif "TRACE" in line_upper:
        return "TRACE"
    else:
        return "INFO"

def get_all_filenames(start_time, end_time):
    """获取所有可用的filename标签值"""
    start_ns = parse_time_to_ns(start_time)
    end_ns = parse_time_to_ns(end_time)
    params = {
        "start": start_ns,
        "end": end_ns
    }
    try:
        response = requests.get(f"{LOKI_API}/label/filename/values", params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("data", [])
    except Exception as e:
        print(f"获取filename标签失败: {e}")
        return []

def query_logs_by_filename(filename, start_time, end_time, limit=10000):
    """按filename从Loki查询日志"""
    start_ns = parse_time_to_ns(start_time)
    end_ns = parse_time_to_ns(end_time)
    params = {
        "query": f'{{filename="{filename}"}}',
        "start": start_ns,
        "end": end_ns,
        "limit": limit,
        "direction": "forward"
    }
    try:
        response = requests.get(f"{LOKI_API}/query_range", params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("data", {}).get("result", [])
    except Exception as e:
        print(f"查询日志失败 ({filename}): {e}")
        return []

def read_mapreduce_logs_from_nfs(filenames):
    """从HDFS NFS挂载点读取MapReduce日志（保留所有行）"""
    all_records = []
    
    for filename in filenames:
        if "application_" not in filename:
            continue
        
        component = extract_component_from_filename(filename)
        node = extract_node_from_filename(filename)
        task_id = extract_task_id_from_filename(filename)
        
        if not task_id:
            continue
        
        nfs_path = filename
        if filename.startswith("/hdfs-nfs"):
            nfs_path = filename
        else:
            nfs_path = os.path.join(HDFS_NFS_MOUNT, filename.lstrip("/"))
        
        if not os.path.exists(nfs_path):
            continue
        
        print(f"  读取NFS文件: {os.path.basename(nfs_path)}")
        
        try:
            with open(nfs_path, "r", encoding="utf-8", errors="ignore") as f:
                line_count = 0
                for line in f:
                    line_count += 1
                    
                    if " " not in line:
                        continue
                    
                    parts = line.split(" ", 2)
                    if len(parts) < 3:
                        continue
                    
                    timestamp_str = parts[0] + " " + parts[1]
                    message = parts[2].strip()
                    
                    try:
                        if "," in parts[1]:
                            line_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                        else:
                            line_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        continue
                    
                    level = extract_level_from_line(message)
                    
                    record = {
                        "timestamp": line_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                        "timestamp_ns": int(line_dt.timestamp() * 1e9),
                        "component": component,
                        "node": node,
                        "filename": filename,
                        "level": level,
                        "task_id": task_id,
                        "message": message
                    }
                    all_records.append(record)
            
            print(f"    读取 {line_count} 行, 有效日志 {len(all_records)} 条")
        except Exception as e:
            print(f"    读取失败: {e}")
            continue
    
    return all_records

def save_logs_to_csv(records_by_type_and_node, output_dir):
    """保存日志到CSV文件，按组件和节点组织"""
    if not records_by_type_and_node:
        return []
    
    saved_files = []
    
    for log_type, records_by_node in sorted(records_by_type_and_node.items()):
        for node, records in sorted(records_by_node.items()):
            if not records:
                continue
            
            records.sort(key=lambda x: int(x["timestamp_ns"]))
            
            node_dir = os.path.join(output_dir, node)
            os.makedirs(node_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{log_type}_{timestamp}.csv"
            filepath = os.path.join(node_dir, filename)
            
            fieldnames = ["timestamp", "timestamp_ns", "component", "node", "filename", "level", "message"]
            if "task_id" in records[0]:
                fieldnames.insert(-1, "task_id")
            
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
                writer.writeheader()
                writer.writerows(records)
            
            print(f"    ✓ {node}/{filename}: {len(records)} 条")
            saved_files.append(filepath)
    
    return saved_files

def save_logs_to_csv_with_task(records_by_task, output_dir):
    """保存带任务ID的日志到CSV文件"""
    if not records_by_task:
        return []
    
    saved_files = []
    
    for task_key, records_by_node in sorted(records_by_task.items()):
        for node, records in sorted(records_by_node.items()):
            if not records:
                continue
            
            records.sort(key=lambda x: int(x["timestamp_ns"]))
            
            node_dir = os.path.join(output_dir, node)
            os.makedirs(node_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"task_{task_key}_{timestamp}.csv"
            filepath = os.path.join(node_dir, filename)
            
            fieldnames = ["timestamp", "timestamp_ns", "component", "node", "filename", "level", "task_id", "message"]
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
                writer.writeheader()
                writer.writerows(records)
            
            print(f"    ✓ {node}/{filename}: {len(records)} 条")
            saved_files.append(filepath)
    
    return saved_files

def collect_component_logs(start_time, end_time, output_dir, limit=10000):
    """收集组件日志（datanode, namenode, nodemanager, resourcemanager）"""
    print(f"\n收集组件日志...")
    
    filenames = get_all_filenames(start_time, end_time)
    
    logs_by_component = defaultdict(lambda: defaultdict(list))
    
    for filename in sorted(filenames):
        component = extract_component_from_filename(filename)
        node = extract_node_from_filename(filename)
        task_id = extract_task_id_from_filename(filename)
        
        if task_id:
            continue
        
        if component == "unknown":
            continue
        
        results = query_logs_by_filename(filename, start_time, end_time, limit)
        
        if not results:
            continue
        
        print(f"  处理: {os.path.basename(filename)}")
        
        for stream in results:
            stream_labels = stream.get("stream", {})
            stream_level = stream_labels.get("level", "unknown")
            
            for timestamp_ns, line in stream.get("values", []):
                try:
                    dt = datetime.fromtimestamp(int(timestamp_ns) / 1e9)
                    
                    record = {
                        "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "timestamp_ns": timestamp_ns,
                        "component": component,
                        "node": node,
                        "filename": filename,
                        "level": stream_level,
                        "message": line
                    }
                    logs_by_component[component][node].append(record)
                except Exception:
                    continue
    
    return save_logs_to_csv(logs_by_component, output_dir)

def collect_mapreduce_logs(filenames, output_dir):
    """收集MapReduce任务日志（不过滤，获取所有行）"""
    print(f"\n收集MapReduce任务日志...")
    
    if not filenames:
        print("  没有找到MapReduce任务日志文件")
        return []
    
    print(f"  找到 {len(filenames)} 个任务日志文件")
    
    records = read_mapreduce_logs_from_nfs(filenames)
    
    if not records:
        print("  没有读取到任何日志记录")
        return []
    
    print(f"  共读取 {len(records)} 条日志记录")
    
    logs_by_task = defaultdict(lambda: defaultdict(list))
    
    for record in records:
        task_id = record["task_id"]
        node = record["node"]
        logs_by_task[task_id][node].append(record)
    
    return save_logs_to_csv_with_task(logs_by_task, output_dir)

def get_time_range_from_mapreduce_logs(filenames):
    """从MapReduce日志获取时间范围"""
    if not filenames:
        return None, None
    
    timestamps = []
    
    for filename in filenames:
        if "application_" not in filename:
            continue
        
        nfs_path = filename
        if filename.startswith("/hdfs-nfs"):
            nfs_path = filename
        else:
            nfs_path = os.path.join(HDFS_NFS_MOUNT, filename.lstrip("/"))
        
        if not os.path.exists(nfs_path):
            continue
        
        try:
            with open(nfs_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    try:
                        if " " not in line:
                            continue
                        
                        timestamp_str = line.split(" ")[0] + " " + line.split(" ")[1]
                        try:
                            line_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                            timestamps.append(line_dt)
                        except ValueError:
                            try:
                                line_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                                timestamps.append(line_dt)
                            except ValueError:
                                continue
                    except Exception:
                        continue
        except Exception:
            continue
    
    if not timestamps:
        return None, None
    
    min_ts = min(timestamps)
    max_ts = max(timestamps)
    
    print(f"  MapReduce日志时间范围: {min_ts} - {max_ts}")
    
    return min_ts, max_ts

def collect_all_logs_with_time_range(start_time, end_time, output_dir, limit=10000):
    """收集所有日志，按时间排序"""
    print(f"\n{'='*60}")
    print(f"开始收集日志")
    print(f"时间范围: {start_time} - {end_time}")
    print(f"输出目录: {output_dir}")
    print(f"{'='*60}\n")
    
    logs_dir = os.path.join(output_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    filenames = get_all_filenames(start_time, end_time)
    
    mapreduce_files = [f for f in filenames if "application_" in f]
    component_files = [f for f in filenames if "application_" not in f]
    
    component_log_files = collect_component_logs(start_time, end_time, logs_dir, limit)
    
    mapreduce_log_files = collect_mapreduce_logs(mapreduce_files, logs_dir)
    
    all_saved_files = component_log_files + mapreduce_log_files
    
    print(f"\n{'='*60}")
    print(f"日志收集完成")
    print(f"文件数: {len(all_saved_files)}")
    print(f"{'='*60}\n")
    
    return all_saved_files

def collect_logs_with_task_determine_time(task_id, output_dir, minutes_before=3, minutes_after=3):
    """根据任务ID收集日志，自动确定时间范围"""
    print(f"\n{'='*60}")
    print(f"根据任务ID收集日志")
    print(f"任务ID: {task_id}")
    print(f"前后时间范围: 故障前{minutes_before}分钟 - 故障后{minutes_after}分钟")
    print(f"输出目录: {output_dir}")
    print(f"{'='*60}\n")
    
    logs_dir = os.path.join(output_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    print(f"查询任务 {task_id} 的日志文件...")
    filenames = get_all_filenames("1970-01-01T00:00:00", "2099-12-31T23:59:59")
    
    task_files = [f for f in filenames if task_id in f]
    
    if not task_files:
        print(f"  警告: 没有找到任务 {task_id} 的日志文件")
    
    task_mapreduce_files = [f for f in task_files if "application_" in f]
    
    min_ts, max_ts = get_time_range_from_mapreduce_logs(task_mapreduce_files)
    
    if min_ts and max_ts:
        start_time = min_ts - timedelta(minutes=minutes_before)
        end_time = max_ts + timedelta(minutes=minutes_after)
        
        start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
        
        print(f"\n调整后的时间范围: {start_str} - {end_str}")
    else:
        print(f"  警告: 无法从MapReduce日志确定时间范围")
        return []
    
    component_files = collect_component_logs(start_str, end_str, logs_dir)
    
    mapreduce_log_files = collect_mapreduce_logs(task_files, logs_dir)
    
    all_saved_files = component_files + mapreduce_log_files
    
    print(f"\n{'='*60}")
    print(f"日志收集完成")
    print(f"时间范围: {start_str} - {end_str}")
    print(f"文件数: {len(all_saved_files)}")
    print(f"{'='*60}\n")
    
    return all_saved_files

def main():
    if len(sys.argv) < 4:
        print("用法: python collect_logs.py <输出目录> <开始时间> <结束时间>")
        print("时间格式: YYYY-MM-DDTHH:MM:SS")
        print("\n新用法（根据任务ID自动确定时间范围）:")
        print("  python collect_logs.py --task <任务ID> <输出目录>")
        sys.exit(1)
    
    output_dir = sys.argv[1]
    start_time = sys.argv[2]
    end_time = sys.argv[3]
    
    os.makedirs(output_dir, exist_ok=True)
    
    if sys.argv[1] == "--task" and len(sys.argv) >= 4:
        task_id = sys.argv[2]
        output_dir = sys.argv[3]
        minutes_before = int(sys.argv[4]) if len(sys.argv) > 4 else 3
        minutes_after = int(sys.argv[5]) if len(sys.argv) > 5 else 3
        os.makedirs(output_dir, exist_ok=True)
        collect_logs_with_task_determine_time(task_id, output_dir, minutes_before, minutes_after)
    else:
        collect_all_logs_with_time_range(start_time, end_time, output_dir)

if __name__ == "__main__":
    main()
