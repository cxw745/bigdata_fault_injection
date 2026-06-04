#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量故障注入调度器

功能:
1. 支持按顺序或随机运行故障注入
2. 支持配置文件定义故障列表和运行次数
3. 自动收集日志和指标数据
4. 详细的运行日志记录
5. 易于扩展新的故障类型
"""

import os
import sys
import argparse
import time
import random
import json
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collect_topology import (
    get_cluster_topology,
    get_fault_config,
    is_fault_nodes_known,
    get_fault_preset_nodes,
    FAULT_CONFIG,
    get_all_nodes
)

from collect_logs import (
    collect_all_logs_with_time_range,
    collect_component_logs,
    collect_mapreduce_logs
)
from collect_metrics import collect_all_metrics

DEFAULT_OUTPUT_BASE = "/tmp/fault_test_results"
LOG_DIR = "/scripts/logs"

DEFAULT_CONFIG = {
    "faults": [
        {
            "type": "data_bloat",
            "count": 2,
            "order": 1,
            "enabled": True,
            "params": {
                "minutes_before": 3,
                "minutes_after": 5,
                "wait_minutes": 1
            }
        },
        {
            "type": "task_fail",
            "count": 2,
            "order": 2,
            "enabled": True,
            "params": {
                "minutes_before": 3,
                "minutes_after": 5,
                "wait_minutes": 1
            }
        },
        {
            "type": "data_skew",
            "count": 2,
            "order": 3,
            "enabled": True,
            "params": {
                "minutes_before": 3,
                "minutes_after": 5,
                "wait_minutes": 1
            }
        },
        {
            "type": "long_tail",
            "count": 1,
            "order": 4,
            "enabled": False,
            "params": {
                "minutes_before": 3,
                "minutes_after": 5,
                "wait_minutes": 1
            }
        }
    ],
    "global_settings": {
        "output_dir": DEFAULT_OUTPUT_BASE,
        "random_order": False,
        "interval_between_faults": 120,
        "interval_between_runs": 300
    }
}

class FaultScheduler:
    """故障注入调度器"""
    
    def __init__(self, config: Dict = None, output_dir: str = DEFAULT_OUTPUT_BASE):
        self.config = config or DEFAULT_CONFIG
        self.output_dir = output_dir
        self.execution_history = []
        self.run_start_time = datetime.now()
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        os.makedirs(LOG_DIR, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(LOG_DIR, f"batch_run_{timestamp}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.log_file = log_file
        
    def log_info(self, msg):
        """记录信息"""
        self.logger.info(msg)
        print(f"[INFO] {msg}")
        
    def log_error(self, msg):
        """记录错误"""
        self.logger.error(msg)
        print(f"[ERROR] {msg}")
        
    def log_success(self, msg):
        """记录成功"""
        self.logger.info(f"✓ {msg}")
        print(f"[SUCCESS] {msg}")
        
    def log_progress(self, current, total, msg):
        """记录进度"""
        percent = (current / total) * 100
        self.logger.info(f"[{current}/{total} ({percent:.1f}%)] {msg}")
        print(f"[PROGRESS] [{current}/{total}] {msg}")
        
    def get_enabled_faults(self) -> List[Dict]:
        """获取启用的故障列表"""
        faults = [f for f in self.config["faults"] if f.get("enabled", True)]
        
        if self.config["global_settings"].get("random_order", False):
            random.shuffle(faults)
        else:
            faults.sort(key=lambda x: x.get("order", 999))
        
        return faults
    
    def run_single_fault(self, fault_type: str, params: Dict = None) -> Dict:
        """运行单个故障注入并收集数据"""
        params = params or {}
        
        self.log_info(f"开始故障注入: {fault_type}")
        
        fault_info = get_fault_config(fault_type)
        self.log_info(f"故障描述: {fault_info.get('description', '未知故障')}")
        
        fault_script = f"/scripts/{fault_type}/inject_{fault_type}.sh"
        
        if not os.path.exists(fault_script):
            self.log_error(f"故障脚本不存在: {fault_script}")
            return {
                "fault_type": fault_type,
                "success": False,
                "error": f"脚本不存在: {fault_script}"
            }
        
        try:
            minutes_before = params.get("minutes_before", 3)
            minutes_after = params.get("minutes_after", 5)
            wait_minutes = params.get("wait_minutes", 1)
            
            task_dir, job_id = run_fault_and_collect(
                fault_script=fault_script,
                output_dir=self.output_dir,
                minutes_before=minutes_before,
                minutes_after=minutes_after,
                wait_minutes=wait_minutes
            )
            
            result = {
                "fault_type": fault_type,
                "job_id": job_id,
                "task_dir": task_dir,
                "success": True,
                "timestamp": datetime.now().isoformat(),
                "affected_nodes": fault_info.get("affected_nodes", []),
                "affected_services": fault_info.get("affected_services", []),
                "nodes_known": fault_info.get("nodes_known", False),
                "params": params
            }
            
            self.execution_history.append(result)
            self.log_success(f"故障注入完成: {fault_type} (Job ID: {job_id})")
            
            return result
            
        except Exception as e:
            self.log_error(f"故障注入失败: {fault_type} - {str(e)}")
            return {
                "fault_type": fault_type,
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def run_all_faults(self) -> List[Dict]:
        """运行所有配置的故障注入"""
        self.log_info("=" * 60)
        self.log_info("开始批量故障注入调度")
        self.log_info("=" * 60)
        
        topology = get_cluster_topology()
        self.log_info(f"集群拓扑 - Master: {topology['master']}, Slaves: {', '.join(topology['slaves'])}")
        
        for fault_config in self.config.get("faults", []):
            if not fault_config.get("enabled", True):
                continue
            
            fault_type = fault_config["type"]
            fault_info = get_fault_config(fault_type)
            nodes_known = fault_info.get("nodes_known", False)
            
            self.log_info(f"故障类型: {fault_type}")
            self.log_info(f"  描述: {fault_info.get('description', '未知')}")
            self.log_info(f"  节点类型: {'已知' if nodes_known else '随机'}")
            self.log_info(f"  影响节点: {', '.join(fault_info.get('affected_nodes', [])) if nodes_known else '运行时动态确定'}")
        
        faults = self.get_enabled_faults()
        
        if not faults:
            self.log_error("没有启用的故障类型")
            return []
        
        total_runs = sum(f["count"] for f in faults)
        self.log_info(f"将执行 {total_runs} 次故障注入")
        self.log_info(f"故障类型: {[f['type'] for f in faults]}")
        
        results = []
        run_counter = 0
        
        for fault_config in faults:
            fault_type = fault_config["type"]
            count = fault_config["count"]
            params = fault_config.get("params", {})
            
            for i in range(count):
                run_counter += 1
                self.log_progress(run_counter, total_runs, f"开始故障注入: {fault_type}")
                self.log_info("-" * 40)
                self.log_info(f"故障类型: {fault_type} ({i+1}/{count})")
                
                result = self.run_single_fault(fault_type, params)
                results.append(result)
                
                if run_counter < total_runs:
                    interval = self.config["global_settings"].get("interval_between_runs", 300)
                    self.log_info(f"等待 {interval} 秒后继续...")
                    time.sleep(interval)
        
        run_end_time = datetime.now()
        run_duration = (run_end_time - self.run_start_time).seconds
        
        self.log_info("=" * 60)
        self.log_info("批量故障注入完成")
        self.log_info(f"总运行时间: {run_duration} 秒")
        self.log_info(f"成功: {sum(1 for r in results if r.get('success', False))}")
        self.log_info(f"失败: {sum(1 for r in results if not r.get('success', False))}")
        self.log_info(f"日志文件: {self.log_file}")
        self.log_info("=" * 60)
        
        self.save_summary(results)
        
        return results
    
    def save_summary(self, results: List[Dict]):
        """保存执行摘要"""
        summary_file = os.path.join(self.output_dir, "execution_summary.json")
        
        summary = {
            "start_time": datetime.now().isoformat(),
            "total_runs": len(results),
            "successful_runs": sum(1 for r in results if r.get("success", False)),
            "failed_runs": sum(1 for r in results if not r.get("success", False)),
            "results": results,
            "config": self.config
        }
        
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        self.log_info(f"执行摘要已保存: {summary_file}")
    
    def print_status(self):
        """打印当前状态"""
        print("\n" + "=" * 60)
        print("故障注入调度器状态")
        print("=" * 60)
        
        topology = get_cluster_topology()
        print(f"\n集群拓扑:")
        print(f"  Master: {topology['master']}")
        print(f"  Slaves: {', '.join(topology['slaves'])}")
        
        print(f"\n配置的故障类型:")
        for fault in self.config["faults"]:
            if fault.get("enabled", True):
                fault_info = get_fault_config(fault["type"])
                nodes_known = fault_info.get("nodes_known", False)
                node_status = "已知节点" if nodes_known else "随机节点"
                print(f"  ✓ {fault['type']}: {fault_info.get('description', '未知')}")
                print(f"    运行次数: {fault['count']}")
                print(f"    节点类型: {node_status}")
                if nodes_known:
                    print(f"    影响节点: {', '.join(fault_info.get('affected_nodes', []))}")
                else:
                    print(f"    影响节点: 运行时动态确定")
        
        print(f"\n输出目录: {self.output_dir}")
        print("=" * 60 + "\n")


def run_fault_and_collect(fault_script, output_dir, minutes_before=3, minutes_after=5, wait_minutes=1):
    """运行故障注入并收集数据"""
    import re
    from datetime import datetime, timedelta
    from collect_logs import (
        get_all_filenames,
        query_logs_by_filename,
        extract_component_from_filename,
        extract_node_from_filename,
        extract_task_id_from_filename
    )
    from collect_metrics import collect_all_metrics
    import csv
    from collections import defaultdict
    
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
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    task_folder = f"{fault_name}_{job_id}_{timestamp}" if job_id else f"{fault_name}_{timestamp}"
    
    task_dir = os.path.join(output_dir, task_folder)
    
    os.makedirs(task_dir, exist_ok=True)
    os.makedirs(os.path.join(task_dir, "logs"), exist_ok=True)
    os.makedirs(os.path.join(task_dir, "metrics"), exist_ok=True)
    
    print(f"收集任务数据到: {task_dir}")
    
    logs_dir = os.path.join(task_dir, "logs")
    all_records = []
    
    filenames = get_all_filenames(collect_start_str, collect_end_str)
    
    logs_by_component = defaultdict(lambda: defaultdict(list))
    task_logs = {}
    
    for filename in sorted(filenames):
        component = extract_component_from_filename(filename)
        node = extract_node_from_filename(filename)
        task_id = extract_task_id_from_filename(filename)
        
        results = query_logs_by_filename(filename, collect_start_str, collect_end_str, 10000)
        
        if not results:
            continue
        
        print(f"处理: {os.path.basename(filename)}")
        
        for stream in results:
            stream_labels = stream.get("stream", {})
            stream_level = stream_labels.get("level", "unknown")
            
            for timestamp_ns, line in stream.get("values", []):
                try:
                    dt = datetime.fromtimestamp(int(timestamp_ns) / 1e9)
                    
                    if task_id:
                        key = f"{component}_{task_id}"
                        if key not in task_logs:
                            task_logs[key] = {}
                        if node not in task_logs[key]:
                            task_logs[key][node] = []
                        
                        record = {
                            "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                            "timestamp_ns": timestamp_ns,
                            "component": component,
                            "node": node,
                            "filename": filename,
                            "level": stream_level,
                            "task_id": task_id,
                            "message": line
                        }
                        task_logs[key][node].append(record)
                    else:
                        key = component
                        if key not in logs_by_component:
                            logs_by_component[key] = {}
                        if node not in logs_by_component[key]:
                            logs_by_component[key][node] = []
                        
                        record = {
                            "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                            "timestamp_ns": timestamp_ns,
                            "component": component,
                            "node": node,
                            "filename": filename,
                            "level": stream_level,
                            "message": line
                        }
                        logs_by_component[key][node].append(record)
                except Exception:
                    continue
    
    log_files = []
    
    for component, logs_by_node in sorted(logs_by_component.items()):
        for node, records in sorted(logs_by_node.items()):
            if not records:
                continue
            
            node_dir = os.path.join(logs_dir, node)
            os.makedirs(node_dir, exist_ok=True)
            
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{component}_{ts}.csv"
            filepath = os.path.join(node_dir, filename)
            
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = ["timestamp", "timestamp_ns", "component", "node", "filename", "level", "message"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
                writer.writeheader()
                writer.writerows(records)
            
            print(f"  ✓ {node}/{filename}: {len(records)} 条")
            log_files.append(filepath)
    
    for task_key, logs_by_node in sorted(task_logs.items()):
        for node, records in sorted(logs_by_node.items()):
            if not records:
                continue
            
            node_dir = os.path.join(logs_dir, node)
            os.makedirs(node_dir, exist_ok=True)
            
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"task_{task_key}_{ts}.csv"
            filepath = os.path.join(node_dir, filename)
            
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = ["timestamp", "timestamp_ns", "component", "node", "filename", "level", "task_id", "message"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
                writer.writeheader()
                writer.writerows(records)
            
            print(f"  ✓ {node}/{filename}: {len(records)} 条")
            log_files.append(filepath)
    
    metric_dir = os.path.join(task_dir, "metrics")
    os.makedirs(metric_dir, exist_ok=True)
    metric_files = collect_all_metrics(collect_start_str, collect_end_str, metric_dir, 15, ["cpu", "memory"])
    
    fault_info = get_fault_config(fault_name)
    topology = get_cluster_topology()
    
    summary_file = os.path.join(task_dir, "collection_summary.txt")
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("故障注入数据收集摘要\n")
        f.write("=" * 60 + "\n")
        f.write(f"收集时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"故障脚本: {fault_script}\n")
        f.write(f"故障类型: {fault_name}\n")
        f.write(f"故障描述: {fault_info.get('description', '未知故障')}\n")
        f.write(f"Job ID: {job_id}\n")
        f.write(f"是否故障注入: 是\n")
        f.write(f"注入节点: {', '.join(fault_info.get('affected_nodes', []))}\n")
        f.write(f"影响服务: {', '.join(fault_info.get('affected_services', []))}\n")
        f.write(f"注入方式: {fault_info.get('injection_method', '未知')}\n")
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
        description="批量故障注入调度器",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "--config",
        default=None,
        help="配置文件路径 (JSON格式)"
    )
    
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_BASE,
        help=f"输出目录 (默认: {DEFAULT_OUTPUT_BASE})"
    )
    
    parser.add_argument(
        "--fault",
        default=None,
        help="只运行单个故障类型"
    )
    
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="故障注入次数 (默认: 1)"
    )
    
    parser.add_argument(
        "--random",
        action="store_true",
        help="随机顺序执行"
    )
    
    parser.add_argument(
        "--status",
        action="store_true",
        help="显示当前配置状态"
    )
    
    args = parser.parse_args()
    
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    if args.status:
        config = DEFAULT_CONFIG.copy()
        if args.config and os.path.exists(args.config):
            with open(args.config, "r") as f:
                config = json.load(f)
        
        scheduler = FaultScheduler(config, output_dir)
        scheduler.print_status()
        sys.exit(0)
    
    if args.fault:
        scheduler = FaultScheduler(DEFAULT_CONFIG, output_dir)
        result = scheduler.run_single_fault(args.fault, {"minutes_before": 3, "minutes_after": 5, "wait_minutes": 1})
        print(f"\n结果: {'成功' if result['success'] else '失败'}")
        sys.exit(0 if result['success'] else 1)
    
    config = DEFAULT_CONFIG.copy()
    if args.config and os.path.exists(args.config):
        with open(args.config, "r") as f:
            config = json.load(f)
    
    config["global_settings"]["random_order"] = args.random
    
    scheduler = FaultScheduler(config, output_dir)
    results = scheduler.run_all_faults()
    
    successful = sum(1 for r in results if r.get("success", False))
    failed = len(results) - successful
    
    print(f"\n" + "=" * 60)
    print(f"批量故障注入完成")
    print(f"成功: {successful}")
    print(f"失败: {failed}")
    print(f"总计: {len(results)}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
