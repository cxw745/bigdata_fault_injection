#!/usr/bin/env python3
"""
统一故障注入调度器 V2

解耦后的架构：
- scheduler_core: 调度逻辑
- log_collector: 日志收集
- metrics_collector: 指标收集
- run_recorder: 运行记录
"""
import os
import sys
import argparse
import logging
import subprocess
import re
import signal
import csv
from datetime import datetime, timedelta
from typing import Dict, Optional, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from unified_config import (
    SCRIPTS_DIR, OUTPUT_BASE, LOG_DIR,
    get_fault_script_path, get_fault_config,
    PROMETHEUS_API, LOKI_API, HDFS_NFS_MOUNT,
    DEFAULT_FAULT_PARAMS
)
from scheduler_core import SchedulerConfig, FaultScheduler, ScheduleMode
from log_collector import LogCollector
from metrics_collector import MetricsCollector
from hibench_manager import HiBenchManager, HIBENCH_DATA_SIZES


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


class UnifiedScheduler:
    """统一调度器"""

    def __init__(self, config: SchedulerConfig, output_dir: str = OUTPUT_BASE, data_size: str = "micro", max_batch_size: int = 50):
        self.config = config
        self.output_dir = output_dir
        self.data_size = data_size
        self.max_batch_size = max_batch_size
        self._shutdown_requested = False

        self._setup_logging()
        self.log_collector = LogCollector(LOKI_API, HDFS_NFS_MOUNT, self.logger)
        self.metrics_collector = MetricsCollector(PROMETHEUS_API, self.logger)
        self.hibench_manager = HiBenchManager(logger=self.logger)
        
        self.current_batch_dir = None
        self.batch_file = "/tmp/batch_scheduler.batch"
        self.records = []
        
        self._setup_signal_handlers()
    
    def _setup_logging(self):
        """设置日志"""
        os.makedirs(LOG_DIR, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(LOG_DIR, f"scheduler_{timestamp}.log")
        
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
    
    def _setup_signal_handlers(self):
        """设置信号处理器"""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信号处理"""
        self.logger.info(f"收到终止信号 {signum}，正在优雅退出...")
        self._shutdown_requested = True
    
    def _create_batch_dir(self) -> str:
        """创建批次目录"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_dir = os.path.join(self.output_dir, f"batch_{timestamp}")
        os.makedirs(batch_dir, exist_ok=True)
        
        with open(self.batch_file, "w") as f:
            f.write(batch_dir)
        
        self.current_batch_dir = batch_dir
        self.logger.info(f"创建批次目录: {batch_dir}")
        return batch_dir
    
    def _init_csv_files(self, batch_dir: str):
        """初始化CSV文件"""
        execution_csv = os.path.join(batch_dir, "execution_records.csv")
        if not os.path.exists(execution_csv):
            with open(execution_csv, "w", newline="", encoding="utf-8") as f:
                fieldnames = [
                    "seq_idx", "fault_type", "start_time", "end_time",
                    "duration", "success", "job_id", "application_id",
                    "logs_dir", "metrics_dir", "error", "nodes", "method", "task_dir"
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
        
        labels_csv = os.path.join(batch_dir, "fault_labels.csv")
        if not os.path.exists(labels_csv):
            with open(labels_csv, "w", newline="", encoding="utf-8") as f:
                fieldnames = ["folder_name", "fault_type", "start_time", "end_time", "label"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
    
    def _record_execution(self, batch_dir: str, record: Dict):
        """记录执行结果到CSV"""
        execution_csv = os.path.join(batch_dir, "execution_records.csv")
        fieldnames = [
            "seq_idx", "fault_type", "start_time", "end_time",
            "duration", "success", "job_id", "application_id",
            "logs_dir", "metrics_dir", "error", "nodes", "method", "task_dir"
        ]
        
        csv_record = {k: record.get(k, "") for k in fieldnames}
        
        # 清理error字段，移除换行符和多余空格，避免破坏CSV格式
        error_msg = csv_record.get("error", "")
        if error_msg:
            # 将多行错误信息合并为一行，保留前200个字符
            error_msg = error_msg.replace('\n', ' ').replace('\r', ' ')
            error_msg = ' '.join(error_msg.split())  # 合并多个空格
            csv_record["error"] = error_msg[:500]  # 限制长度
        
        with open(execution_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writerow(csv_record)
        
        self.records.append(record)
    
    def _record_fault_label(self, batch_dir: str, record: Dict):
        """记录故障标签"""
        labels_csv = os.path.join(batch_dir, "fault_labels.csv")
        
        folder_name = os.path.basename(record.get("task_dir", ""))
        fault_type = record.get("fault_type", "unknown")
        start_time = record.get("start_time", "")
        end_time = record.get("end_time", "")
        label = FAULT_LABELS.get(fault_type, 0)
        
        with open(labels_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["folder_name", "fault_type", "start_time", "end_time", "label"])
            writer.writerow({
                "folder_name": folder_name,
                "fault_type": fault_type,
                "start_time": start_time,
                "end_time": end_time,
                "label": label
            })
    
    def execute_fault(self, fault_type: str, seq_idx: int, batch_dir: str) -> Dict:
        """
        执行故障注入
        
        Args:
            fault_type: 故障类型 (或 "normal" 表示无故障)
            seq_idx: 序列索引
            batch_dir: 批次目录
            
        Returns:
            执行结果字典
        """
        start_time = datetime.now()
        
        if fault_type in ("normal", "wordcount"):
            self.logger.info("执行正常任务（无故障）")
            return self._execute_normal_task(start_time, seq_idx, batch_dir)
        
        self.logger.info(f"执行故障注入: {fault_type}")
        
        fault_info = get_fault_config(fault_type)
        fault_script = get_fault_script_path(fault_type)
        
        if not os.path.exists(fault_script):
            return self._create_error_result(start_time, fault_type, f"脚本不存在: {fault_script}", seq_idx, batch_dir)
        
        try:
            job_id, application_id = self._run_fault_script(fault_script, fault_type)
            
            task_dir = self._create_task_dir(fault_type, batch_dir, application_id)
            
            self._wait_for_task(application_id)
            
            end_time = datetime.now()
            duration = (end_time - start_time).seconds
            
            logs_dir = os.path.join(task_dir, "logs")
            metrics_dir = os.path.join(task_dir, "metrics")
            os.makedirs(logs_dir, exist_ok=True)
            os.makedirs(metrics_dir, exist_ok=True)
            
            # 扩大日志收集时间窗口：任务开始前10分钟到结束后15分钟
            # 给Loki足够的时间来收集和索引日志
            collect_start = (start_time - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
            collect_end = (end_time + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%S")
            
            self.logger.info(f"日志收集时间窗口: {collect_start} ~ {collect_end}")
            
            log_files, log_count = self.log_collector.collect_from_loki(
                collect_start, collect_end, logs_dir, application_id
            )
            
            if application_id:
                mr_files, mr_count = self.log_collector.collect_mapreduce_logs(
                    application_id, logs_dir
                )
                log_files.extend(mr_files)
                log_count += mr_count
            
            metric_files = self.metrics_collector.collect_fault_specific_metrics(
                fault_type,
                int((start_time - timedelta(minutes=3)).timestamp()),
                int((end_time + timedelta(minutes=5)).timestamp()),
                metrics_dir
            )

            result = {
                "seq_idx": seq_idx,
                "fault_type": fault_type,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration": duration,
                "success": True,
                "job_id": job_id or "",
                "application_id": application_id or "",
                "logs_dir": logs_dir,
                "metrics_dir": metrics_dir,
                "error": "",
                "nodes": ",".join(fault_info.get("affected_nodes", [])),
                "method": fault_info.get("injection_method", "unknown"),
                "task_dir": task_dir
            }
            
            self._record_execution(batch_dir, result)
            self._record_fault_label(batch_dir, result)
            
            return result

        except Exception as e:
            task_dir = self._create_task_dir(fault_type, batch_dir)
            result = self._create_error_result(start_time, fault_type, str(e), seq_idx, batch_dir, task_dir)
            self._record_execution(batch_dir, result)
            self._record_fault_label(batch_dir, result)
            return result
    
    def _execute_normal_task(self, start_time: datetime, seq_idx: int, batch_dir: str) -> Dict:
        """执行正常任务（无故障）"""
        hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
        
        cmd = f"""
        {hadoop_home}/bin/hadoop jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
            -D mapreduce.job.name=wordcount_benchmark \
            -D mapreduce.job.maps=24 \
            -D mapreduce.job.reduces=8 \
            -input /HiBench/HiBench/Wordcount/Input \
            -output /user/hadoop/normal_output_{int(start_time.timestamp())} \
            -mapper "python3 mapper.py" \
            -reducer "python3 reducer.py" \
            -file {SCRIPTS_DIR}/common_mapreduce/mapper.py \
            -file {SCRIPTS_DIR}/common_mapreduce/reducer.py
        """
        
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=600
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).seconds
            
            job_id = None
            application_id = None
            output_lines = result.stdout.splitlines() + result.stderr.splitlines()
            for line in output_lines:
                match = re.search(r'application_(\d+_\d+)', line)
                if match:
                    application_id = f"application_{match.group(1)}"
                    job_id = application_id
            
            self.logger.info(f"Wordcount任务完成, application_id: {application_id}")
            
            # 等待日志写入NFS
            self._wait_for_task(application_id)
            
            task_dir = self._create_task_dir("wordcount", batch_dir, application_id)
            
            logs_dir = os.path.join(task_dir, "logs")
            metrics_dir = os.path.join(task_dir, "metrics")
            os.makedirs(logs_dir, exist_ok=True)
            os.makedirs(metrics_dir, exist_ok=True)
            
            # 扩大日志收集时间窗口：任务开始前10分钟到结束后15分钟
            collect_start = (start_time - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
            collect_end = (end_time + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%S")
            
            self.logger.info(f"日志收集时间窗口: {collect_start} ~ {collect_end}")
            
            log_files, log_count = self.log_collector.collect_from_loki(
                collect_start, collect_end, logs_dir, application_id
            )
            
            if application_id:
                mr_files, mr_count = self.log_collector.collect_mapreduce_logs(
                    application_id, logs_dir
                )
                log_files.extend(mr_files)
                log_count += mr_count
            
            metric_files = self.metrics_collector.collect_fault_specific_metrics(
                "normal",
                int((start_time - timedelta(minutes=3)).timestamp()),
                int((end_time + timedelta(minutes=5)).timestamp()),
                metrics_dir
            )

            exec_result = {
                "seq_idx": seq_idx,
                "fault_type": "wordcount",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration": duration,
                "success": result.returncode == 0,
                "job_id": job_id or "",
                "application_id": application_id or "",
                "logs_dir": logs_dir,
                "metrics_dir": metrics_dir,
                "error": result.stderr if result.returncode != 0 else "",
                "nodes": "",
                "method": "标准WordCount实现",
                "task_dir": task_dir
            }
            
            self._record_execution(batch_dir, exec_result)
            self._record_fault_label(batch_dir, exec_result)
            
            return exec_result
            
        except subprocess.TimeoutExpired:
            task_dir = self._create_task_dir("wordcount", batch_dir)
            result = self._create_error_result(start_time, "wordcount", "任务超时", seq_idx, batch_dir, task_dir)
            self._record_execution(batch_dir, result)
            self._record_fault_label(batch_dir, result)
            return result
        except Exception as e:
            task_dir = self._create_task_dir("wordcount", batch_dir)
            result = self._create_error_result(start_time, "wordcount", str(e), seq_idx, batch_dir, task_dir)
            self._record_execution(batch_dir, result)
            self._record_fault_label(batch_dir, result)
            return result
    
    def _run_fault_script(self, script_path: str, fault_type: str) -> tuple:
        """运行故障脚本"""
        script_type = get_fault_config(fault_type).get("script_type", "sh")
        
        if script_type == "py":
            process = subprocess.Popen(
                ["python3", script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
        else:
            process = subprocess.Popen(
                ["bash", script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
        
        output = []
        for line in iter(process.stdout.readline, ''):
            output.append(line)
            print(line, end='')
        process.wait()
        
        job_id = None
        application_id = None
        for line in output:
            match = re.search(r'job_(\d+_\d+)', line)
            if match:
                job_id = f"job_{match.group(1)}"
            match = re.search(r'application_(\d+_\d+)', line)
            if match:
                application_id = f"application_{match.group(1)}"
                if not job_id:
                    job_id = application_id
        
        return job_id, application_id
    
    def _wait_for_task(self, application_id: str = None):
        """等待任务完成，并确保日志已写入NFS"""
        import time
        # 基础等待时间：10秒
        time.sleep(10)
        
        # 如果提供了application_id，等待日志文件出现在NFS上
        if application_id:
            mr_logs_base = "/hdfs-nfs/tmp/logs"
            app_id_parts = application_id.split('_')
            if len(app_id_parts) >= 3:
                app_id_num = app_id_parts[-1]
                potential_paths = [
                    f"{mr_logs_base}/ubuntu/bucket-cxw745-logs-tfile/{app_id_num}/{application_id}",
                    f"{mr_logs_base}/{application_id}",
                ]
                
                # 最多等待60秒，检查日志文件是否出现
                for i in range(12):  # 12 * 5 = 60秒
                    for path in potential_paths:
                        if os.path.exists(path) and os.listdir(path):
                            self.logger.info(f"日志文件已就绪: {path}")
                            return
                    self.logger.info(f"等待日志文件出现... ({i+1}/12)")
                    time.sleep(5)
                
                self.logger.warning(f"日志文件未在预期时间内出现: {application_id}")
    
    def _create_task_dir(self, fault_type: str, batch_dir: str, application_id: str = None) -> str:
        """创建任务目录"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if application_id:
            task_dir = os.path.join(batch_dir, f"{fault_type}_{application_id}_{timestamp}")
        else:
            task_dir = os.path.join(batch_dir, f"{fault_type}_{timestamp}")
        
        os.makedirs(task_dir, exist_ok=True)
        return task_dir
    
    def _create_error_result(self, start_time: datetime, fault_type: str, error: str, seq_idx: int, batch_dir: str, task_dir: str = "") -> Dict:
        """创建错误结果"""
        end_time = datetime.now()
        return {
            "seq_idx": seq_idx,
            "fault_type": fault_type,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration": (end_time - start_time).seconds,
            "success": False,
            "job_id": "",
            "application_id": "",
            "logs_dir": "",
            "metrics_dir": "",
            "error": error,
            "nodes": "",
            "method": "",
            "task_dir": task_dir
        }
    
    def prepare_data(self) -> bool:
        """
        准备测试数据

        Returns:
            True if success
        """
        self.logger.info("=" * 60)
        self.logger.info("准备测试数据")
        self.logger.info("=" * 60)

        if self.data_size not in HIBENCH_DATA_SIZES:
            self.logger.error(f"未知的数据大小: {self.data_size}")
            return False

        config = HIBENCH_DATA_SIZES[self.data_size]
        self.logger.info(f"数据大小: {config.name} ({config.description})")

        success = self.hibench_manager.generate_data(config)

        if success:
            info = self.hibench_manager.get_current_data_info()
            self.logger.info(f"✓ 数据准备完成: {info['size_formatted']}")
        else:
            self.logger.error("✗ 数据准备失败")

        return success

    def run(self):
        """运行调度器"""
        self.logger.info("=" * 60)
        self.logger.info("统一故障注入调度器 V2 启动")
        self.logger.info("=" * 60)
        self.logger.info(f"输出目录: {self.output_dir}")
        self.logger.info(f"数据大小: {self.data_size}")
        self.logger.info(f"调度模式: {self.config.mode.value}")
        self.logger.info(f"每批次上限: {self.max_batch_size}")
        
        if self.config.mode.value == "sequential":
            self.logger.info(f"执行序列: {self.config.sequence}")
        else:
            self.logger.info(f"故障类型: {self.config.fault_types}")
            self.logger.info(f"故障数量: {self.config.fault_counts}")
            self.logger.info(f"Normal数量: {self.config.normal_count}")
        
        self.logger.info(f"间隔范围: {self.config.interval_min}s - {self.config.interval_max}s")
        self.logger.info(f"最大日志数: {self.config.max_logs or '无限制'}")
        self.logger.info(f"最大时长: {self.config.max_duration or '无限制'}s")

        if not self.prepare_data():
            self.logger.error("数据准备失败，退出调度")
            return []
        
        sequence = self._generate_sequence()
        total_tasks = len(sequence)
        
        self.logger.info(f"总任务数: {total_tasks}")
        
        batch_count = (total_tasks + self.max_batch_size - 1) // self.max_batch_size
        self.logger.info(f"将分为 {batch_count} 个批次执行")
        
        results = []
        task_idx = 0
        
        for batch_num in range(batch_count):
            if self._shutdown_requested:
                break
            
            batch_start = batch_num * self.max_batch_size
            batch_end = min((batch_num + 1) * self.max_batch_size, total_tasks)
            batch_tasks = batch_end - batch_start
            
            self.logger.info("=" * 60)
            self.logger.info(f"开始批次 {batch_num + 1}/{batch_count} (任务 {batch_start + 1}-{batch_end})")
            self.logger.info("=" * 60)
            
            batch_dir = self._create_batch_dir()
            self._init_csv_files(batch_dir)
            
            for i in range(batch_tasks):
                if self._shutdown_requested:
                    break
                
                fault_type = sequence[task_idx]
                task_idx += 1
                
                self.logger.info(f"[{task_idx}/{total_tasks}] 执行: {fault_type}")
                
                try:
                    result = self.execute_fault(fault_type, task_idx, batch_dir)
                    results.append(result)
                    
                except Exception as e:
                    self.logger.error(f"执行 {fault_type} 失败: {e}")
                    result = self._create_error_result(datetime.now(), fault_type, str(e), task_idx, batch_dir)
                    self._record_execution(batch_dir, result)
                    self._record_fault_label(batch_dir, result)
                    results.append(result)
                
                if i < batch_tasks - 1 and not self._shutdown_requested:
                    import time
                    import random
                    interval = random.randint(self.config.interval_min, self.config.interval_max)
                    self.logger.info(f"等待 {interval} 秒后执行下一次...")
                    time.sleep(interval)
            
            self._save_batch_summary(batch_dir, batch_num + 1, batch_count)
        
        self.logger.info("=" * 60)
        self.logger.info("调度完成")
        self.logger.info(f"总执行次数: {len(results)}")
        self.logger.info(f"成功: {sum(1 for r in results if r.get('success', False))}")
        self.logger.info(f"失败: {sum(1 for r in results if not r.get('success', False))}")
        self.logger.info(f"日志文件: {self.log_file}")
        self.logger.info("=" * 60)
        
        return results
    
    def _generate_sequence(self) -> List[str]:
        """生成执行序列"""
        import random
        sequence = []
        
        if self.config.mode == ScheduleMode.SEQUENTIAL and self.config.sequence:
            for fault_type, count in self.config.sequence:
                for _ in range(count):
                    sequence.append(fault_type)
        else:
            for fault_type in self.config.fault_types:
                count = self.config.fault_counts.get(fault_type, 1)
                for _ in range(count):
                    sequence.append(fault_type)
            
            if self.config.include_normal:
                for _ in range(self.config.normal_count):
                    sequence.append("wordcount")
        
        random.shuffle(sequence)
        return sequence
    
    def _save_batch_summary(self, batch_dir: str, batch_num: int, total_batches: int):
        """保存批次摘要"""
        import json
        
        summary = {
            "batch_number": batch_num,
            "total_batches": total_batches,
            "batch_dir": batch_dir,
            "completed_at": datetime.now().isoformat()
        }
        
        summary_file = os.path.join(batch_dir, "batch_summary.json")
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"批次摘要已保存: {summary_file}")


def parse_sequence(sequence_str: str) -> list:
    """解析序列字符串，格式: fault_type:count,fault_type:count,..."""
    sequence = []
    for item in sequence_str.split(","):
        parts = item.strip().split(":")
        if len(parts) == 2:
            fault_type = parts[0].strip()
            count = int(parts[1].strip())
            sequence.append((fault_type, count))
    return sequence


def parse_fault_counts(counts_str: str) -> dict:
    """解析故障数量字符串，格式: fault_type:count,fault_type:count,..."""
    counts = {}
    for item in counts_str.split(","):
        parts = item.strip().split(":")
        if len(parts) == 2:
            fault_type = parts[0].strip()
            count = int(parts[1].strip())
            counts[fault_type] = count
    return counts


def main():
    parser = argparse.ArgumentParser(description="统一故障注入调度器 V2")
    parser.add_argument("--output-dir", default=OUTPUT_BASE, help=f"输出目录 (默认: {OUTPUT_BASE})")
    parser.add_argument("--data-size", default="tiny",
                        choices=list(HIBENCH_DATA_SIZES.keys()),
                        help="数据大小 (tiny/small/large/huge/gigantic)")
    
    # 调度模式
    parser.add_argument("--mode", type=str, default="sequential",
                        choices=["sequential", "random"],
                        help="调度模式: sequential(顺序) 或 random(随机)")
    
    # 顺序模式参数
    parser.add_argument("--sequence", type=str,
                        help="顺序模式序列，格式: fault_type:count,fault_type:count,... 例如: data_skew:2,task_fail:1,normal:1")
    
    # 随机模式参数
    parser.add_argument("--fault-types", nargs="+", default=["data_skew", "task_fail", "long_tail"],
                        help="随机模式 - 故障类型列表")
    parser.add_argument("--fault-counts", type=str,
                        help="随机模式 - 每种故障数量，格式: fault_type:count,fault_type:count,... 例如: data_skew:3,task_fail:2")
    parser.add_argument("--total-count", type=int, default=0,
                        help="随机模式 - 总执行次数（0表示根据fault-counts计算）")
    parser.add_argument("--normal-count", type=int, default=0,
                        help="随机模式 - normal任务数量")
    parser.add_argument("--include-normal", action="store_true", default=True,
                        help="随机模式 - 包含无故障执行")
    
    # 通用参数
    parser.add_argument("--max-logs", type=int, help="收集到多少条日志停止")
    parser.add_argument("--max-duration", type=int, help="最大运行时长（秒）")
    parser.add_argument("--interval-min", type=int, default=60, help="最小间隔（秒）")
    parser.add_argument("--interval-max", type=int, default=300, help="最大间隔（秒）")
    parser.add_argument("--max-batch-size", type=int, default=50, help="每批次最大任务数")

    args = parser.parse_args()

    # 根据模式创建配置
    if args.mode == "sequential":
        # 顺序模式
        if args.sequence:
            sequence = parse_sequence(args.sequence)
        else:
            # 默认序列
            sequence = [(fault, 1) for fault in args.fault_types]
            if args.include_normal:
                sequence.append(("normal", 1))
        
        config = SchedulerConfig(
            mode=ScheduleMode.SEQUENTIAL,
            sequence=sequence,
            max_logs=args.max_logs,
            max_duration=args.max_duration,
            interval_min=args.interval_min,
            interval_max=args.interval_max
        )
    else:
        # 随机模式
        fault_counts = parse_fault_counts(args.fault_counts) if args.fault_counts else {}
        
        # 如果没有指定数量，默认每种1个
        if not fault_counts:
            fault_counts = {fault: 1 for fault in args.fault_types}
        
        config = SchedulerConfig(
            mode=ScheduleMode.RANDOM,
            fault_types=args.fault_types,
            fault_counts=fault_counts,
            total_count=args.total_count,
            include_normal=args.include_normal,
            normal_count=args.normal_count,
            max_logs=args.max_logs,
            max_duration=args.max_duration,
            interval_min=args.interval_min,
            interval_max=args.interval_max
        )

    scheduler = UnifiedScheduler(config, args.output_dir, args.data_size, args.max_batch_size)
    scheduler.run()


if __name__ == "__main__":
    main()
