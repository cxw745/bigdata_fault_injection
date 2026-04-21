#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
from datetime import datetime, timedelta
from collect_logs import collect_all_logs
from collect_metrics import collect_all_metrics
from collect_data import setup_task_directory

def quick_test(output_dir, minutes=5, task_id=None):
    """快速测试数据收集"""
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=minutes)
    
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    
    output_dir = setup_task_directory(output_dir, task_id)
    logs_dir = os.path.join(output_dir, "logs")
    metrics_dir = os.path.join(output_dir, "metrics")
    
    print(f"\n{'='*60}")
    print(f"快速测试 - 数据收集")
    print(f"时间范围: {start_str} - {end_str}")
    print(f"输出目录: {output_dir}")
    if task_id:
        print(f"任务ID: {task_id}")
    print(f"{'='*60}\n")
    
    print("测试日志收集...")
    log_files = collect_all_logs(start_str, end_str, logs_dir)
    
    print("\n测试指标收集...")
    metric_files = collect_all_metrics(start_str, end_str, metrics_dir, step=15, categories=["cpu", "memory"])
    
    print(f"\n{'='*60}")
    print(f"测试结果")
    total_log_files = sum(len(files) for files in log_files.values())
    print(f"日志文件: {total_log_files} 个")
    print(f"指标文件: {len(metric_files)} 个")
    print(f"{'='*60}\n")
    
    return log_files, metric_files, output_dir

def main():
    parser = argparse.ArgumentParser(
        description="数据收集快速测试",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "output_dir",
        help="输出目录路径"
    )
    
    parser.add_argument(
        "--minutes",
        type=int,
        default=5,
        help="测试时间范围（分钟，默认5）"
    )
    
    parser.add_argument(
        "--task-id",
        dest="task_id",
        help="任务ID，用于创建任务文件夹"
    )
    
    args = parser.parse_args()
    
    output_dir = os.path.abspath(args.output_dir)
    
    log_files, metric_files, actual_dir = quick_test(output_dir, args.minutes, args.task_id)
    
    if log_files and metric_files:
        print(f"✓ 测试成功！")
        print(f"数据保存到: {actual_dir}")
        sys.exit(0)
    else:
        print("✗ 测试失败！")
        sys.exit(1)

if __name__ == "__main__":
    main()
