#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
import os
import sys
import re
from collections import defaultdict
from datetime import datetime

IP_TO_HOSTNAME = {
    "10.10.3.183": "cpf-1",
    "10.10.1.96": "cpf-2",
    "10.10.3.222": "cpf-3",
    "10.10.0.176": "cpf-4"
}

STATIC_LOG_TYPES = {
    "hadoop_logs", "hdfs_logs", "yarn_logs", "nodemanager", 
    "resourcemanager", "datanode", "namenode", "historyserver"
}

def extract_hostname_from_message(message):
    """从日志消息中提取节点信息"""
    if not message:
        return "unknown"
    
    ip_pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
    match = re.search(ip_pattern, message)
    
    if match:
        ip = match.group(1)
        return IP_TO_HOSTNAME.get(ip, ip)
    
    return "unknown"

def classify_logs_by_node(input_filepath, output_dir):
    """读取日志文件并按节点分类"""
    print(f"\n{'='*60}")
    print(f"日志分类")
    print(f"输入文件: {input_filepath}")
    print(f"输出目录: {output_dir}")
    print(f"{'='*60}\n")
    
    if not os.path.exists(input_filepath):
        print(f"错误: 文件不存在 {input_filepath}")
        return
    
    logs_by_node_and_type = defaultdict(lambda: defaultdict(list))
    
    line_count = 0
    with open(input_filepath, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            line_count += 1
            message = row.get("message", "")
            original_hostname = row.get("hostname", "unknown")
            log_type = row.get("log_type", "unknown")
            
            hostname = extract_hostname_from_message(message)
            if hostname == "unknown":
                hostname = original_hostname
            
            row["hostname"] = hostname
            logs_by_node_and_type[hostname][log_type].append(row)
    
    print(f"处理了 {line_count} 条日志记录\n")
    
    os.makedirs(output_dir, exist_ok=True)
    
    stats = {}
    
    for hostname, logs_by_type in sorted(logs_by_node_and_type.items()):
        hostname_dir = os.path.join(output_dir, hostname)
        os.makedirs(hostname_dir, exist_ok=True)
        
        stats[hostname] = {}
        
        for log_type, records in sorted(logs_by_type.items()):
            if not records:
                continue
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if log_type in STATIC_LOG_TYPES:
                filename = f"{log_type}_{timestamp}.csv"
            else:
                filename = f"task_{log_type}_{timestamp}.csv"
            
            filepath = os.path.join(hostname_dir, filename)
            
            with open(filepath, "w", newline="", encoding="utf-8") as outfile:
                fieldnames = ["timestamp", "timestamp_ns", "log_type", "hostname", "level", "message"]
                writer = csv.DictWriter(outfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
                writer.writeheader()
                writer.writerows(records)
            
            stats[hostname][log_type] = {
                "filepath": filepath,
                "count": len(records)
            }
            
            print(f"  ✓ {hostname}/{filename}: {len(records)} 条")
    
    print(f"\n{'='*60}")
    print(f"分类完成")
    print(f"{'='*60}\n")
    
    total_records = sum(
        sum(records_count for records_count in type_stats.values())
        for type_stats in stats.values()
    )
    
    total_files = sum(
        len(type_stats) for type_stats in stats.values()
    )
    
    print(f"总计:")
    print(f"  - 分类文件: {total_files} 个")
    print(f"  - 总记录数: {total_records}")
    print(f"  - 输出目录: {output_dir}")
    print(f"\n节点分布:")
    for hostname, type_stats in sorted(stats.items()):
        node_total = sum(count for count in type_stats.values())
        print(f"  - {hostname}: {node_total} 条")
    
    return stats

def quick_classify(input_dir, output_dir):
    """快速分类目录下所有CSV文件"""
    print(f"\n{'='*60}")
    print(f"快速分类")
    print(f"输入目录: {input_dir}")
    print(f"输出目录: {output_dir}")
    print(f"{'='*60}\n")
    
    os.makedirs(output_dir, exist_ok=True)
    
    csv_files = []
    for root, dirs, files in os.walk(input_dir):
        for f in files:
            if f.endswith(".csv") and f != "collection_summary.txt":
                filepath = os.path.join(root, f)
                csv_files.append(filepath)
    
    if not csv_files:
        print("没有找到CSV文件")
        return
    
    print(f"找到 {len(csv_files)} 个CSV文件\n")
    
    all_records = []
    for filepath in sorted(csv_files):
        filename = os.path.basename(filepath)
        print(f"读取: {filename}")
        
        with open(filepath, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                message = row.get("message", "")
                original_hostname = row.get("hostname", "unknown")
                log_type = row.get("log_type", "unknown")
                
                hostname = extract_hostname_from_message(message)
                if hostname == "unknown":
                    hostname = original_hostname
                
                row["hostname"] = hostname
                all_records.append(row)
    
    if not all_records:
        print("没有日志记录")
        return
    
    print(f"\n总共 {len(all_records)} 条记录，按节点分类...\n")
    
    logs_by_node_and_type = defaultdict(lambda: defaultdict(list))
    
    for record in all_records:
        hostname = record.get("hostname", "unknown")
        log_type = record.get("log_type", "unknown")
        logs_by_node_and_type[hostname][log_type].append(record)
    
    for hostname, logs_by_type in sorted(logs_by_node_and_type.items()):
        hostname_dir = os.path.join(output_dir, hostname)
        os.makedirs(hostname_dir, exist_ok=True)
        
        for log_type, records in sorted(logs_by_type.items()):
            if not records:
                continue
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if log_type in STATIC_LOG_TYPES:
                filename = f"{log_type}_{timestamp}.csv"
            else:
                filename = f"task_{log_type}_{timestamp}.csv"
            
            filepath = os.path.join(hostname_dir, filename)
            
            with open(filepath, "w", newline="", encoding="utf-8") as outfile:
                fieldnames = ["timestamp", "timestamp_ns", "log_type", "hostname", "level", "message"]
                writer = csv.DictWriter(outfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
                writer.writeheader()
                writer.writerows(records)
            
            print(f"  ✓ {hostname}/{filename}: {len(records)} 条")
    
    print(f"\n分类完成!")

def main():
    if len(sys.argv) < 3:
        print("用法:")
        print("  1. python parse_logs.py <输入CSV> <输出目录>")
        print("  2. python parse_logs.py --quick <输入目录> <输出目录>")
        print("\n示例:")
        print("  python parse_logs.py ./logs.csv ./classified")
        print("  python parse_logs.py --quick ./raw_logs ./classified")
        sys.exit(1)
    
    if sys.argv[1] == "--quick":
        input_dir = sys.argv[2]
        output_dir = sys.argv[3] if len(sys.argv) > 3 else "./classified"
        quick_classify(input_dir, output_dir)
    else:
        input_filepath = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else "./classified"
        classify_logs_by_node(input_filepath, output_dir)

if __name__ == "__main__":
    main()
