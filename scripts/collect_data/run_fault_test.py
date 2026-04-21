#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import re
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collect_logs import (
    get_all_filenames,
    query_logs_by_filename,
    extract_component_from_filename,
    extract_node_from_filename,
    extract_task_id_from_filename
)
from collect_metrics import collect_all_metrics
from collect_topology import (
    get_cluster_topology,
    get_fault_config,
    is_fault_nodes_known,
    get_fault_preset_nodes,
    get_master_node,
    get_slave_nodes,
    get_all_nodes
)

DEFAULT_OUTPUT_BASE = "/tmp/fault_test_results"
DEFAULT_STEP = 15
DEFAULT_WAIT_MINUTES = 1

def setup_task_directory(base_dir, fault_type, job_id=None):
    """设置任务输出目录，层级：任务/logs/节点/数据.csv"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if job_id:
        task_folder = f"{fault_type}_{job_id}_{timestamp}"
    else:
        task_folder = f"{fault_type}_{timestamp}"
    
    task_dir = os.path.join(base_dir, task_folder)
    
    os.makedirs(task_dir, exist_ok=True)
    os.makedirs(os.path.join(task_dir, "logs"), exist_ok=True)
    os.makedirs(os.path.join(task_dir, "metrics"), exist_ok=True)
    
    return task_dir

def collect_logs_for_task(start_time, end_time, output_dir, limit=10000):
    """收集指定时间范围内的所有日志"""
    import csv
    from collections import defaultdict
    from collect_logs import (
        collect_component_logs,
        collect_mapreduce_logs,
        get_all_filenames
    )
    
    print(f"\n{'='*60}")
    print(f"开始收集日志")
    print(f"时间范围: {start_time} - {end_time}")
    print(f"输出目录: {output_dir}")
    print(f"{'='*60}\n")
    
    logs_dir = os.path.join(output_dir, "logs")
    
    all_saved_files = []
    
    component_files = collect_component_logs(start_time, end_time, logs_dir, limit)
    all_saved_files.extend(component_files)
    
    all_filenames = get_all_filenames(start_time, end_time)
    mapreduce_files = [f for f in all_filenames if "application_" in f]
    all_saved_files.extend(collect_mapreduce_logs(mapreduce_files, logs_dir))
    
    print(f"\n{'='*60}")
    print(f"日志收集完成")
    print(f"文件数: {len(all_saved_files)}")
    print(f"{'='*60}\n")
    
    return all_saved_files

def collect_metrics_for_task(start_time, end_time, output_dir, step=15, categories=None):
    """收集指定时间范围内的指标"""
    metrics_dir = os.path.join(output_dir, "metrics")
    os.makedirs(metrics_dir, exist_ok=True)
    
    return collect_all_metrics(start_time, end_time, metrics_dir, step, categories)

def run_fault_and_collect(fault_script, output_dir, minutes_before=3, minutes_after=5, wait_minutes=1):
    """运行故障注入并收集数据"""
    import subprocess
    
    print(f"\n{'='*60}")
    print(f"运行故障注入")
    print(f"脚本: {fault_script}")
    print(f"完成后等待: {wait_minutes} 分钟")
    print(f"{'='*60}\n")
    
    start_time = datetime.now()
    
    process = subprocess.Popen(
        ["bash", fault_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    output = []
    for line in iter(process.stdout.readline, ''):
        output.append(line)
        print(line, end='')
    
    process.wait()
    end_time = datetime.now()
    
    job_id = None
    for line in output:
        match = re.search(r'job_(\d+_\d+)', line)
        if match:
            job_id = f"job_{match.group(1)}"
            break
    
    print(f"\n{'='*60}")
    print(f"故障注入完成")
    print(f"Job ID: {job_id}")
    print(f"执行时间: {(end_time - start_time).seconds} 秒")
    print(f"{'='*60}\n")
    
    if wait_minutes > 0:
        print(f"等待 {wait_minutes} 分钟，让Loki抓取jobhistory日志...")
        time.sleep(wait_minutes * 60)
        print("等待完成，开始收集数据\n")
    
    collect_start = end_time - timedelta(minutes=minutes_before)
    collect_end = datetime.now() + timedelta(minutes=minutes_after)
    
    collect_start_str = collect_start.strftime("%Y-%m-%dT%H:%M:%S")
    collect_end_str = collect_end.strftime("%Y-%m-%dT%H:%M:%S")
    
    fault_name = os.path.basename(fault_script).split('.')[0].replace("inject_", "")
    task_dir = setup_task_directory(output_dir, fault_name, job_id)
    
    print(f"收集任务数据到: {task_dir}")
    
    log_files = collect_logs_for_task(collect_start_str, collect_end_str, task_dir)
    metric_files = collect_metrics_for_task(collect_start_str, collect_end_str, task_dir)
    
    summary_file = os.path.join(task_dir, "collection_summary.txt")
    
    fault_config = get_fault_config(fault_name)
    topology = get_cluster_topology()
    
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("故障注入数据收集摘要\n")
        f.write("=" * 60 + "\n")
        f.write(f"收集时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"故障脚本: {fault_script}\n")
        f.write(f"故障类型: {fault_name}\n")
        f.write(f"故障描述: {fault_config['description']}\n")
        f.write(f"Job ID: {job_id}\n")
        f.write(f"是否故障注入: 是\n")
        f.write(f"注入节点: {', '.join(fault_config['affected_nodes'])}\n")
        f.write(f"影响服务: {', '.join(fault_config['affected_services'])}\n")
        f.write(f"注入方式: {fault_config['injection_method']}\n")
        f.write(f"执行时间: {(end_time - start_time).seconds} 秒\n")
        f.write(f"等待时间: {wait_minutes} 分钟\n")
        f.write(f"日志时间范围: {collect_start_str} - {collect_end_str}\n")
        f.write(f"日志文件: {len(log_files)} 个\n")
        f.write(f"指标文件: {len(metric_files)} 个\n")
        
        f.write("\n" + "=" * 60 + "\n")
        f.write("集群拓扑信息\n")
        f.write("=" * 60 + "\n")
        f.write(f"Master节点: {topology['master']}\n")
        f.write(f"Slave节点: {', '.join(topology['slaves'])}\n")
        f.write(f"所有节点: {', '.join(topology['all_nodes'])}\n")
        f.write("\n节点角色:\n")
        for node, roles in topology['roles'].items():
            f.write(f"  {node}: {', '.join(roles)}\n")
        
        log_stats = {}
        for log_file in log_files:
            filename = os.path.basename(log_file)
            node = os.path.basename(os.path.dirname(log_file))
            if filename not in log_stats:
                log_stats[filename] = {}
            line_count = sum(1 for _ in open(log_file)) - 1
            log_stats[filename][node] = line_count
        
        f.write("\n日志文件统计:\n")
        f.write("-" * 60 + "\n")
        total_log_lines = 0
        for filename, nodes in sorted(log_stats.items()):
            f.write(f"  {filename}:\n")
            for node, count in sorted(nodes.items()):
                f.write(f"    - {node}: {count} 条\n")
                total_log_lines += count
        
        metric_stats = {}
        for metric_file in metric_files:
            filename = os.path.basename(metric_file)
            node = os.path.basename(os.path.dirname(metric_file))
            line_count = sum(1 for _ in open(metric_file)) - 1
            metric_stats[node] = line_count
        
        f.write("\n指标文件统计:\n")
        f.write("-" * 60 + "\n")
        total_metric_lines = 0
        for node, count in sorted(metric_stats.items()):
            f.write(f"  - {node}: {count} 条\n")
            total_metric_lines += count
        
        f.write("\n日志文件列表:\n")
        f.write("-" * 60 + "\n")
        for log_file in sorted(log_files):
            f.write(f"  - {log_file}\n")
        
        f.write("\n指标文件列表:\n")
        f.write("-" * 60 + "\n")
        for metric_file in sorted(metric_files):
            f.write(f"  - {metric_file}\n")
        
        f.write("\n汇总:\n")
        f.write("-" * 60 + "\n")
        f.write(f"总日志文件: {len(log_files)} 个\n")
        f.write(f"总日志记录: {total_log_lines} 条\n")
        f.write(f"总指标文件: {len(metric_files)} 个\n")
        f.write(f"总指标记录: {total_metric_lines} 条\n")
        f.write(f"总数据量: {len(log_files) + len(metric_files)} 个文件\n")
    
    print(f"\n{'='*60}")
    print(f"任务完成")
    print(f"输出目录: {task_dir}")
    print(f"日志文件: {len(log_files)} 个")
    print(f"指标文件: {len(metric_files)} 个")
    print(f"{'='*60}\n")
    
    return task_dir, job_id

def main():
    parser = argparse.ArgumentParser(
        description="故障注入与数据收集",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "--fault",
        required=True,
        help="故障类型 (data_skew, data_bloat, task_fail, 等)"
    )
    
    parser.add_argument(
        "--fault-script",
        dest="fault_script",
        help="故障注入脚本路径"
    )
    
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_BASE,
        help=f"输出目录 (默认: {DEFAULT_OUTPUT_BASE})"
    )
    
    parser.add_argument(
        "--minutes-before",
        type=int,
        default=3,
        help="故障开始前收集分钟数 (默认: 3)"
    )
    
    parser.add_argument(
        "--minutes-after",
        type=int,
        default=5,
        help="故障结束后收集分钟数 (默认: 5)"
    )
    
    parser.add_argument(
        "--wait-minutes",
        type=int,
        default=DEFAULT_WAIT_MINUTES,
        help=f"故障结束后等待分钟数，让Loki抓取jobhistory (默认: {DEFAULT_WAIT_MINUTES})"
    )
    
    args = parser.parse_args()
    
    if args.fault_script:
        fault_script = args.fault_script
    else:
        fault_script = f"/scripts/{args.fault}/inject_{args.fault}.sh"
    
    if not os.path.exists(fault_script):
        print(f"错误: 故障脚本不存在: {fault_script}")
        sys.exit(1)
    
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    task_dir, job_id = run_fault_and_collect(
        fault_script,
        output_dir,
        args.minutes_before,
        args.minutes_after,
        args.wait_minutes
    )
    
    print(f"\n数据已保存到: {task_dir}")

if __name__ == "__main__":
    main()
