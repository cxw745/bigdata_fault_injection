#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
from datetime import datetime, timedelta
from collect_logs import (
    get_all_filenames,
    query_logs_by_filename,
    extract_component_from_filename,
    extract_node_from_filename,
    extract_task_id_from_filename,
    collect_all_logs_with_time_range,
    collect_component_logs,
    collect_mapreduce_logs,
    get_time_range_from_mapreduce_logs
)
from collect_metrics import collect_all_metrics, METRICS_CONFIG

DEFAULT_OUTPUT_BASE = "./collected_data"
DEFAULT_STEP = 15

def parse_time(time_str):
    """解析时间字符串"""
    try:
        return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            print(f"时间格式错误: {time_str}")
            print("支持的格式: YYYY-MM-DDTHH:MM:SS 或 YYYY-MM-DD HH:MM:SS")
            sys.exit(1)

def setup_task_directory(base_dir, task_id=None):
    """设置任务输出目录"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if task_id:
        task_folder = f"{task_id}_{timestamp}"
    else:
        task_folder = f"task_{timestamp}"
    
    output_dir = os.path.join(base_dir, task_folder)
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "logs"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "metrics"), exist_ok=True)
    
    return output_dir

def collect_logs_only(start_time, end_time, output_dir, limit=5000):
    """只收集日志"""
    logs_dir = os.path.join(output_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    return collect_all_logs_with_time_range(start_time, end_time, logs_dir, limit)

def collect_metrics_only(start_time, end_time, output_dir, step=15, categories=None):
    """只收集指标"""
    metrics_dir = os.path.join(output_dir, "metrics")
    os.makedirs(metrics_dir, exist_ok=True)
    return collect_all_metrics(start_time, end_time, metrics_dir, step, categories)

def collect_all_data(start_time, end_time, output_dir, step=15, categories=None, limit=5000):
    """收集所有数据（日志和指标）"""
    print(f"\n{'='*60}")
    print(f"数据收集任务")
    print(f"开始时间: {start_time}")
    print(f"结束时间: {end_time}")
    print(f"指标采集间隔: {step}秒")
    print(f"输出目录: {output_dir}")
    print(f"{'='*60}\n")
    
    logs_dir = os.path.join(output_dir, "logs")
    metrics_dir = os.path.join(output_dir, "metrics")
    
    os.makedirs(logs_dir, exist_ok=True)
    os.makedirs(metrics_dir, exist_ok=True)
    
    log_files = collect_all_logs_with_time_range(start_time, end_time, logs_dir, limit)
    
    if categories is None:
        categories = ["cpu", "memory"]
    
    metric_files = collect_all_metrics(start_time, end_time, metrics_dir, step, categories)
    
    summary_file = os.path.join(output_dir, "collection_summary.txt")
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(f"数据收集摘要\n")
        f.write(f"{'='*40}\n")
        f.write(f"收集时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"日志时间范围: {start_time} - {end_time}\n")
        f.write(f"指标时间范围: {start_time} - {end_time}\n")
        f.write(f"指标采集间隔: {step}秒\n")
        f.write(f"\n收集的文件:\n")
        f.write(f"日志文件: {len(log_files)} 个\n")
        for hostname, files in log_files.items():
            for log_file in files:
                f.write(f"  - {hostname}: {log_file}\n")
        f.write(f"\n指标文件: {len(metric_files)} 个\n")
        for metric_file in metric_files:
            f.write(f"  - {metric_file}\n")
    
    print(f"\n{'='*60}")
    print(f"数据收集完成")
    print(f"摘要文件: {summary_file}")
    total_log_files = sum(len(files) for files in log_files.values())
    print(f"日志文件: {total_log_files} 个")
    print(f"指标文件: {len(metric_files)} 个")
    print(f"{'='*60}\n")
    
    return {
        "logs": log_files,
        "metrics": metric_files,
        "summary": summary_file
    }

def quick_collect(base_dir, minutes=30, step=15, categories=None, task_id=None, collect_logs=True, collect_metrics=True):
    """快速收集最近N分钟的数据"""
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=minutes)
    
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    
    output_dir = setup_task_directory(base_dir, task_id)
    
    logs_dir = os.path.join(output_dir, "logs")
    metrics_dir = os.path.join(output_dir, "metrics")
    
    results = {}
    
    if collect_logs:
        results["logs"] = collect_all_logs_with_time_range(start_str, end_str, logs_dir)
    
    if collect_metrics:
        if categories is None:
            categories = ["cpu", "memory"]
        results["metrics"] = collect_all_metrics(start_str, end_str, metrics_dir, step, categories)
    
    return results, output_dir

def get_available_metric_categories():
    """获取可用的指标类别"""
    return list(METRICS_CONFIG.keys())

def main():
    parser = argparse.ArgumentParser(
        description="Hadoop集群日志和指标收集工具",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "action",
        choices=["all", "logs", "metrics", "quick"],
        help="操作类型:\n"
             "  all    - 收集日志和指标\n"
             "  logs   - 只收集日志\n"
             "  metrics - 只收集指标\n"
             "  quick  - 快速收集最近N分钟数据"
    )
    
    parser.add_argument(
        "output_dir",
        help="输出目录路径"
    )
    
    parser.add_argument(
        "--start",
        dest="start_time",
        help="开始时间 (格式: YYYY-MM-DDTHH:MM:SS)"
    )
    
    parser.add_argument(
        "--end",
        dest="end_time",
        help="结束时间 (格式: YYYY-MM-DDTHH:MM:SS)"
    )
    
    parser.add_argument(
        "--minutes",
        type=int,
        default=30,
        help="快速模式下的分钟数 (默认: 30)"
    )
    
    parser.add_argument(
        "--step",
        type=int,
        default=DEFAULT_STEP,
        help=f"指标采集间隔秒数 (默认: {DEFAULT_STEP})"
    )
    
    parser.add_argument(
        "--categories",
        nargs="+",
        help=f"指标类别，可用类别: {' '.join(get_available_metric_categories())}"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="日志查询限制条数 (默认: 5000)"
    )
    
    parser.add_argument(
        "--task-id",
        dest="task_id",
        help="任务ID，用于创建任务文件夹"
    )
    
    args = parser.parse_args()
    
    output_dir = os.path.abspath(args.output_dir)
    
    if args.action == "quick":
        results, actual_dir = quick_collect(output_dir, args.minutes, args.step, args.categories, args.task_id)
        print(f"\n数据已保存到: {actual_dir}")
    else:
        if not args.start_time or not args.end_time:
            print("错误: --start 和 --end 参数必需")
            parser.print_help()
            sys.exit(1)
        
        start_time = args.start_time
        end_time = args.end_time
        
        if args.task_id:
            output_dir = setup_task_directory(output_dir, args.task_id)
        
        if args.action == "all":
            collect_all_data(start_time, end_time, output_dir, args.step, args.categories, args.limit)
        elif args.action == "logs":
            collect_logs_only(start_time, end_time, output_dir, args.limit)
        elif args.action == "metrics":
            collect_metrics_only(start_time, end_time, output_dir, args.step, args.categories)

if __name__ == "__main__":
    main()
