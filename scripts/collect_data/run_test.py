#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import re
from datetime import datetime, timedelta
from collect_logs import collect_all_logs
from collect_metrics import collect_all_metrics

FAULT_SCRIPT = "/scripts/data_skew/inject_data_skew.sh"
HADOOP_HOME = "/opt/hadoop-3.4.1"

def run_fault_injection(fault_script):
    """运行故障注入脚本并捕获Job ID"""
    print(f"\n{'='*60}")
    print(f"启动故障注入")
    print(f"脚本: {fault_script}")
    print(f"{'='*60}\n")
    
    output = os.popen(f"bash {fault_script} 2>&1").read()
    print(output)
    
    print(f"\n{'='*60}")
    print(f"故障注入完成")
    print(f"{'='*60}\n")
    
    job_id = None
    match = re.search(r'job_(\d+_\d+)', output)
    if match:
        job_id = f"job_{match.group(1)}"
    
    return job_id, output

def cleanup_output_dir(output_dir):
    """清理旧的输出目录"""
    if os.path.exists(output_dir):
        print(f"清理旧的输出目录: {output_dir}")
        import shutil
        shutil.rmtree(output_dir)

def run_complete_test(output_dir, minutes_before=5, task_id=None):
    """运行完整的测试流程"""
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=minutes_before)
    
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    
    print(f"\n{'='*60}")
    print(f"数据收集测试")
    print(f"时间范围: {start_str} - {end_str}")
    print(f"输出目录: {output_dir}")
    if task_id:
        print(f"任务ID: {task_id}")
    print(f"{'='*60}\n")
    
    print("收集日志...")
    log_files = collect_all_logs(start_str, end_str, output_dir)
    
    print("\n收集指标...")
    metric_files = collect_all_metrics(start_str, end_str, output_dir, step=15, categories=["cpu", "memory"])
    
    print(f"\n{'='*60}")
    print(f"测试结果")
    total_log_files = sum(len(files) for files in log_files.values())
    print(f"日志文件: {total_log_files} 个")
    print(f"指标文件: {len(metric_files)} 个")
    print(f"{'='*60}\n")
    
    return log_files, metric_files

def verify_collected_data(log_files, metric_files):
    """验证收集的数据"""
    print(f"\n{'='*60}")
    print(f"数据验证")
    print(f"{'='*60}\n")
    
    all_valid = True
    
    if log_files:
        total_files = sum(len(files) for files in log_files.values())
        print(f"✓ 日志文件 ({total_files} 个):")
        for hostname, files in log_files.items():
            for log_file in files:
                if os.path.exists(log_file):
                    size = os.path.getsize(log_file)
                    print(f"  - {hostname}: {os.path.basename(log_file)} ({size} bytes)")
                    if size == 0:
                        print(f"    ⚠ 文件为空")
                        all_valid = False
                else:
                    print(f"  ✗ {hostname}: 文件不存在: {log_file}")
                    all_valid = False
    else:
        print("✗ 没有收集到日志文件")
        all_valid = False
    
    if metric_files:
        print(f"\n✓ 指标文件 ({len(metric_files)} 个):")
        for metric_file in metric_files:
            if os.path.exists(metric_file):
                size = os.path.getsize(metric_file)
                filename = os.path.basename(metric_file)
                hostname = os.path.basename(os.path.dirname(metric_file))
                print(f"  - {hostname}: {filename} ({size} bytes)")
                if size == 0:
                    print(f"    ⚠ 文件为空")
                    all_valid = False
            else:
                print(f"  ✗ 文件不存在: {metric_file}")
                all_valid = False
    else:
        print("✗ 没有收集到指标文件")
        all_valid = False
    
    print(f"\n{'='*60}")
    if all_valid:
        print(f"✓ 所有数据验证通过")
    else:
        print(f"⚠ 部分数据验证失败")
    print(f"{'='*60}\n")
    
    return all_valid

def main():
    parser = argparse.ArgumentParser(
        description="故障注入与数据收集完整测试",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "--output-dir",
        default="./test_output",
        help="输出目录 (默认: ./test_output)"
    )
    
    parser.add_argument(
        "--minutes",
        type=int,
        default=10,
        help="收集最近N分钟的数据 (默认: 10)"
    )
    
    parser.add_argument(
        "--run-fault",
        action="store_true",
        help="是否运行故障注入脚本"
    )
    
    parser.add_argument(
        "--fault-script",
        default=FAULT_SCRIPT,
        help=f"故障注入脚本路径 (默认: {FAULT_SCRIPT})"
    )
    
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="是否清理输出目录"
    )
    
    args = parser.parse_args()
    
    output_dir = os.path.abspath(args.output_dir)
    
    if args.cleanup:
        cleanup_output_dir(output_dir)
    
    task_id = None
    if args.run_fault:
        task_id, output = run_fault_injection(args.fault_script)
        input("\n按 Enter 键继续收集数据...")
    
    log_files, metric_files = run_complete_test(output_dir, args.minutes, task_id)
    verify_collected_data(log_files, metric_files)

if __name__ == "__main__":
    main()
