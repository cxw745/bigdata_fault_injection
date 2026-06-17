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
import json
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

def _sample_interval(interval_min, interval_max, shape_k=1.5, scale_lambda=180):
    import math, random
    u = random.random()
    while u == 0.0:
        u = random.random()
    raw = scale_lambda * (-math.log(u)) ** (1.0 / shape_k)
    interval = max(interval_min, min(int(raw), interval_max))
    return interval



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
    "network_latency": 8,
    "log_level_change": 9,
    "process_restart": 10,
    "heartbeat_timeout": 11,
    "disk_error": 12,
    "disk_full": 13,
    "network_loss": 14
}

WORKLOAD_TYPES = ["wordcount", "sort", "terasort"]

WORKLOAD_INPUT_PATHS = {
    "wordcount": "/HiBench/HiBench/Wordcount/Input",
    "sort": "/HiBench/HiBench/Sort/Input",
    "terasort": "/HiBench/HiBench/Terasort/Input"
}


class UnifiedScheduler:
    """统一调度器"""

    def __init__(self, config: SchedulerConfig, output_dir: str = OUTPUT_BASE, data_size: str = "small", max_batch_size: int = 50, workload: str = "random"):
        self.config = config
        self.output_dir = output_dir
        self.data_size = data_size
        self.workload = workload
        self.max_batch_size = max_batch_size
        self.skip_prepare = False
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
    
    def _get_workload_type(self) -> str:
        """获取当前任务使用的workload类型"""
        if self.workload == "random":
            import random
            return random.choice(WORKLOAD_TYPES)
        return self.workload




    def _save_collection_statistics(self, batch_dir: str):
        """保存采集统计摘要到批次目录"""
        from collections import Counter

        labels_csv = os.path.join(batch_dir, "fault_labels.csv")
        if not os.path.exists(labels_csv):
            return

        fault_type_counts = Counter()
        success_counts = {"success": 0, "failed": 0}
        total_duration = 0
        duration_by_type = {}

        with open(labels_csv, "r", encoding="utf-8") as f:
            import csv as csv_mod
            reader = csv_mod.DictReader(f)
            for row in reader:
                ft = row.get("fault_type", "unknown")
                fault_type_counts[ft] += 1

        exec_csv = os.path.join(batch_dir, "execution_records.csv")
        if os.path.exists(exec_csv):
            with open(exec_csv, "r", encoding="utf-8") as f:
                import csv as csv_mod
                reader = csv_mod.DictReader(f)
                for row in reader:
                    if row.get("success") == "True":
                        success_counts["success"] += 1
                    else:
                        success_counts["failed"] += 1

                    ft = row.get("fault_type", "unknown")
                    dur = int(row.get("duration", "0"))
                    total_duration += dur
                    if ft not in duration_by_type:
                        duration_by_type[ft] = []
                    duration_by_type[ft].append(dur)

        total = success_counts["success"] + success_counts["failed"]
        stats = {
            "collection_summary": {
                "total_tasks": total,
                "successful_tasks": success_counts["success"],
                "failed_tasks": success_counts["failed"],
                "success_rate": round(success_counts["success"] / total * 100, 2) if total > 0 else 0,
                "total_duration_seconds": total_duration,
                "total_duration_hours": round(total_duration / 3600, 2),
                "average_task_duration_seconds": round(total_duration / total, 2) if total > 0 else 0,
            },
            "fault_type_distribution": {},
            "duration_statistics": {},
        }

        for ft, count in sorted(fault_type_counts.items()):
            stats["fault_type_distribution"][ft] = {
                "count": count,
                "percentage": round(count / total * 100, 2) if total > 0 else 0,
            }

        for ft, durations in sorted(duration_by_type.items()):
            stats["duration_statistics"][ft] = {
                "count": len(durations),
                "min_seconds": min(durations),
                "max_seconds": max(durations),
                "avg_seconds": round(sum(durations) / len(durations), 2),
                "median_seconds": sorted(durations)[len(durations) // 2],
            }

        stats_path = os.path.join(batch_dir, "collection_statistics.json")
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)

        self.logger.info(f"采集统计已保存: {stats_path}")

    def _save_experiment_config(self, batch_dir: str, sequence: list):
        """保存实验配置到批次目录，供论文复现使用"""
        from collections import Counter

        fault_counts = Counter()
        for fault_type, count in sequence:
            fault_counts[fault_type] += count

        total = sum(fault_counts.values())
        normal_count = fault_counts.get("normal", 0)
        fault_count = total - normal_count

        config = {
            "experiment": {
                "name": f"FaultLLM_Data_Collection_{datetime.now().strftime('%Y%m%d')}",
                "description": "FaultLLM故障诊断数据集采集实验",
                "version": "2.0",
                "date": datetime.now().strftime("%Y-%m-%d"),
            },
            "data_collection_config": {
                "total_samples": total,
                "normal_samples": normal_count,
                "fault_samples": fault_count,
                "normal_ratio": round(normal_count / total, 4) if total > 0 else 0,
                "fault_ratio": round(fault_count / total, 4) if total > 0 else 0,
                "workload_type": self.workload,
                "data_size": self.data_size,
                "interval_range_seconds": [self.config.interval_min, self.config.interval_max],
                "interval_distribution": {
                    "type": "Weibull",
                    "shape_k": 1.5,
                    "scale_lambda": 180,
                    "reference": "DSN2017-Wang-DataCenterFailures-PLOSONE2017-Liu-WeibullCloud"
                },
                "max_batch_size": self.max_batch_size,
            },
            "fault_distribution": {
                "design": "加权分布，基于真实场景故障频率",
                "reference": "基于Tsinghua/Baidu DSN 2017真实故障分布数据",
                "fault_types": {},
            },
            "hadoop_cluster": {
                "master_node": "cxw-1 (10.10.0.82)",
                "slave_nodes": ["cxw-2 (10.10.0.83)", "cxw-3 (10.10.0.84)", "cxw-4 (10.10.0.85)"],
                "total_nodes": 4,
                "hadoop_version": "3.3.6",
                "hdfs_replication": 3,
            },
            "fault_injection_tools": {
                "code_level": ["custom_mapper_reducer", "hadoop_streaming"],
                "process_level": ["SIGSTOP/SIGCONT", "process_kill_restart"],
                "network_level": ["chaosblade", "iptables"],
                "system_level": ["hdfs_permission", "hadoop_loglevel_servlet"],
            },
            "data_sources": {
                "metrics": "Prometheus + NodeExporter + JMXExporter",
                "logs": "Loki + HDFS Log Aggregation",
                "topology": "YARN ResourceManager API",
            },
            "fault_categories": {
                "baseline": ["normal"],
                "data_distribution": ["data_skew", "data_bloat"],
                "task_execution": ["task_fail", "long_tail"],
                "scheduling": ["wait_time", "runtime_delta"],
                "node_management": ["exit_time", "process_restart", "heartbeat_timeout"],
                "network": ["network_latency", "network_loss"],
                "log_anomaly": ["log_level_change"],
                "hardware": ["disk_error", "disk_full"],
            }
        }

        for ft, count in sorted(fault_counts.items()):
            config["fault_distribution"]["fault_types"][ft] = {
                "count": count,
                "percentage": round(count / total * 100, 2) if total > 0 else 0,
                "label": FAULT_LABELS.get(ft, 0),
                "description": get_fault_config(ft).get("description", "") if ft not in ("normal", "wordcount") else "正常任务（无故障注入）",
                "category": get_fault_config(ft).get("category", "baseline") if ft not in ("normal", "wordcount") else "baseline",
            }

        config_path = os.path.join(batch_dir, "experiment_config.json")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

        self.logger.info(f"实验配置已保存: {config_path}")

    def _save_fault_injection_detail(self, task_dir: str, fault_type: str, result: Dict, start_time: datetime, end_time: datetime):
        """保存故障注入详细参数到任务目录，供论文分析使用"""
        fault_info = get_fault_config(fault_type) if fault_type not in ("normal", "wordcount") else {}

        detail = {
            "task_id": result.get("application_id", ""),
            "fault_type": fault_type,
            "fault_label": FAULT_LABELS.get(fault_type, 0),
            "fault_category": fault_info.get("category", "baseline"),
            "description": fault_info.get("description", "正常任务"),
            "injection_method": fault_info.get("injection_method", ""),
            "affected_nodes": fault_info.get("affected_nodes", []),
            "affected_services": fault_info.get("affected_services", []),
            "inject_stage": fault_info.get("inject_stage", ""),
            "time_window": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": (end_time - start_time).seconds,
                "log_collection_start": (start_time - timedelta(minutes=10)).isoformat(),
                "log_collection_end": (end_time + timedelta(minutes=15)).isoformat(),
            },
            "workload": {
                "type": self.workload if self.workload != "random" else "wordcount",
                "data_size": self.data_size,
                "input_path": WORKLOAD_INPUT_PATHS.get(self.workload if self.workload != "random" else "wordcount", ""),
            },
            "cluster": {
                "master": "cxw-1",
                "slaves": ["cxw-2", "cxw-3", "cxw-4"],
                "total_nodes": 4,
            },
            "execution": {
                "success": result.get("success", False),
                "application_id": result.get("application_id", ""),
                "job_id": result.get("job_id", ""),
                "error": result.get("error", ""),
            },
            "data_collection": {
                "logs_dir": result.get("logs_dir", ""),
                "metrics_dir": result.get("metrics_dir", ""),
            }
        }

        detail_path = os.path.join(task_dir, "fault_injection_detail.json")
        with open(detail_path, "w", encoding="utf-8") as f:
            json.dump(detail, f, indent=2, ensure_ascii=False)

        self.logger.info(f"故障注入详情已保存: {detail_path}")

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
            
            # Recovery wait: ensure fault effects are fully dissipated
            # Prevents residual effects (e.g. ChaosBlade CPU limits) from affecting next task
            import time as _recovery_time
            _RECOVERY_WAIT = 45
            self.logger.info(f"Recovery wait: {_RECOVERY_WAIT}s to ensure fault effects fully dissipated")
            _recovery_time.sleep(_RECOVERY_WAIT)
            
            # Post-recovery safety check: clean up residual files and check disk space
            try:
                for _chk_node in ["cxw-1", "cxw-2", "cxw-3", "cxw-4"]:
                    try:
                        # Clean ChaosBlade residual files
                        subprocess.run(
                            f"ssh -o ConnectTimeout=5 {_chk_node} \"echo ubuntu | sudo -S rm -f /chaos_filldisk.log.dat /chaos_burnio.read /chaos_burnio.write /tmp/disk_stress /tmp/disk_fill_stress 2>/dev/null || true\"",
                            shell=True, capture_output=True, text=True, timeout=10
                        )
                        # Check disk usage
                        _df_result = subprocess.run(
                            f"ssh -o ConnectTimeout=5 {_chk_node} \"df / | tail -1 | awk '{{print \\$5}}' | tr -d '%'\"",
                            shell=True, capture_output=True, text=True, timeout=10
                        )
                        _usage = int(_df_result.stdout.strip()) if _df_result.stdout.strip().isdigit() else 0
                        if _usage > 90:
                            self.logger.warning(f"⚠ 磁盘使用率过高 on {_chk_node}: {_usage}%")
                    except Exception:
                        pass
            except Exception:
                pass

            # Post-recovery DataNode health check: ensure all DataNodes are alive
            try:
                _dn_report = subprocess.run(
                    f"{self.hadoop_home}/bin/hdfs dfsadmin -report 2>&1",
                    shell=True, capture_output=True, text=True, timeout=15
                )
                _live_match = __import__('re').search(r'Live datanodes \((\d+)\)', _dn_report.stdout)
                _live_count = int(_live_match.group(1)) if _live_match else 0
                if _live_count < 3:
                    self.logger.warning(f"⚠ DataNode不足: Live={_live_count}, 尝试重启...")
                    # Find dead nodes and restart them
                    for _dn_node in ["cxw-2", "cxw-3", "cxw-4"]:
                        try:
                            _dn_check = subprocess.run(
                                f"ssh -o ConnectTimeout=5 {_dn_node} 'jps | grep DataNode | wc -l'",
                                shell=True, capture_output=True, text=True, timeout=10
                            )
                            if _dn_check.stdout.strip() == "0":
                                self.logger.info(f"  重启DataNode on {_dn_node}...")
                                subprocess.run(
                                    f"ssh -o ConnectTimeout=5 {_dn_node} '/opt/hadoop/bin/hdfs --daemon stop datanode 2>/dev/null || true'",
                                    shell=True, capture_output=True, text=True, timeout=10
                                )
                                import time; time.sleep(2)
                                subprocess.run(
                                    f"ssh -o ConnectTimeout=5 {_dn_node} '/opt/hadoop/bin/hdfs --daemon start datanode'",
                                    shell=True, capture_output=True, text=True, timeout=10
                                )
                                self.logger.info(f"  ✔ DataNode on {_dn_node} 已重启")
                                import time; time.sleep(10)
                        except Exception as _dn_e:
                            self.logger.warning(f"  ⚠ DataNode重启失败 on {_dn_node}: {_dn_e}")
                    # Verify after restart
                    import time; time.sleep(15)
                    _dn_report2 = subprocess.run(
                        f"{self.hadoop_home}/bin/hdfs dfsadmin -report 2>&1",
                        shell=True, capture_output=True, text=True, timeout=15
                    )
                    _live_match2 = __import__('re').search(r'Live datanodes \((\d+)\)', _dn_report2.stdout)
                    _live_count2 = int(_live_match2.group(1)) if _live_match2 else 0
                    if _live_count2 >= 3:
                        self.logger.info(f"  ✔ DataNode恢复成功: Live={_live_count2}")
                    else:
                        self.logger.warning(f"  ⚠ DataNode仍未完全恢复: Live={_live_count2}")
            except Exception as _dn_ex:
                self.logger.warning(f"⚠ DataNode健康检查异常: {_dn_ex}")
            
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
                try:
                    disk_f, disk_c = self.log_collector.collect_rm_nm_logs_from_disk(
                        start_time, end_time, logs_dir)
                    log_files.extend(disk_f)
                    log_count += disk_c
                except Exception:
                    pass
            
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
            self._save_fault_injection_detail(task_dir, fault_type, result, start_time, end_time)
            
            return result

        except Exception as e:
            task_dir = self._create_task_dir(fault_type, batch_dir)
            result = self._create_error_result(start_time, fault_type, str(e), seq_idx, batch_dir, task_dir)
            self._record_execution(batch_dir, result)
            return result
    
    def _execute_normal_task(self, start_time: datetime, seq_idx: int, batch_dir: str) -> Dict:
        """执行正常任务（无故障）"""
        workload_type = self._get_workload_type()
        hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
        input_path = WORKLOAD_INPUT_PATHS.get(workload_type, WORKLOAD_INPUT_PATHS["wordcount"])
        output_path = f"/user/hadoop/normal_{workload_type}_output_{int(start_time.timestamp())}"

        if workload_type == "terasort":
            cmd = f"""
            {hadoop_home}/bin/hadoop jar {hadoop_home}/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar terasort                 -Dmapreduce.job.maps=24                 -Dmapreduce.job.reduces=8                 {input_path} {output_path}
            """
        elif workload_type == "sort":
            cmd = f"""
            {hadoop_home}/bin/hadoop jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
                -D mapreduce.job.name=sort_benchmark \
                -D mapreduce.job.maps=24 \
                -D mapreduce.job.reduces=8 \
                -inputformat org.apache.hadoop.mapred.SequenceFileInputFormat \
                -input {input_path} \
                -output {output_path} \
                -mapper "cat" \
                -reducer "cat"
            """
        else:
            cmd = f"""
            {hadoop_home}/bin/hadoop jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
                -D mapreduce.job.name=wordcount_benchmark \
                -D mapreduce.job.maps=24 \
                -D mapreduce.job.reduces=8 \
                -inputformat org.apache.hadoop.mapred.SequenceFileInputFormat \
                -input {input_path} \
                -output {output_path} \
                -mapper "python3 mapper.py" \
                -reducer "python3 reducer.py" \
                -file {SCRIPTS_DIR}/common_mapreduce/mapper.py \
                -file {SCRIPTS_DIR}/common_mapreduce/reducer.py
            """

        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=900
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

            # Fallback: query YARN for recently finished apps if app_id not found
            if not application_id:
                try:
                    import subprocess as _sp
                    _cutoff = int(start_time.timestamp())
                    _result = subprocess.run(
                        ["/opt/hadoop/bin/yarn", "application", "-list", "-appStates", "RUNNING,FINISHED,FAILED,KILLED"],
                        capture_output=True, text=True, timeout=15
                    )
                    for _line in _result.stdout.splitlines():
                        _parts = _line.split()
                        if len(_parts) >= 4 and _parts[1].isdigit():
                            _app_ts = int(_parts[1]) // 1000  # YARN timestamp is in ms
                            if abs(_app_ts - _cutoff) < 1200:  # within 20 min window
                                _match = re.search(r'(application_\d+_\d+)', _line)
                                if _match:
                                    application_id = _match.group(1)
                                    job_id = application_id
                                    self.logger.info(f"找到应用ID (YARN回退): {application_id}")
                                    break
                except Exception as _e:
                    self.logger.debug(f"YARN回退查询失败: {_e}")

            self.logger.info(f"{workload_type}任务完成, application_id: {application_id}")

            self._wait_for_task(application_id)

            task_dir = self._create_task_dir(workload_type, batch_dir, application_id)

            logs_dir = os.path.join(task_dir, "logs")
            metrics_dir = os.path.join(task_dir, "metrics")
            os.makedirs(logs_dir, exist_ok=True)
            os.makedirs(metrics_dir, exist_ok=True)

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
                try:
                    disk_f, disk_c = self.log_collector.collect_rm_nm_logs_from_disk(
                        start_time, end_time, logs_dir)
                    log_files.extend(disk_f)
                    log_count += disk_c
                except Exception:
                    pass

            metric_files = self.metrics_collector.collect_fault_specific_metrics(
                "normal",
                int((start_time - timedelta(minutes=3)).timestamp()),
                int((end_time + timedelta(minutes=5)).timestamp()),
                metrics_dir
            )

            exec_result = {
                "seq_idx": seq_idx,
                "fault_type": workload_type,
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
                "method": f"标准{workload_type}实现",
                "task_dir": task_dir
            }

            self._record_execution(batch_dir, exec_result)
            self._record_fault_label(batch_dir, exec_result)
            self._save_fault_injection_detail(task_dir, workload_type, exec_result, start_time, end_time)

            return exec_result

        except subprocess.TimeoutExpired:
            self.logger.info("普通任务超时，尝试收集已生成的日志和指标...")
            try:
                end_time = datetime.now()
                duration = (end_time - start_time).seconds

                job_id = None
                application_id = None
                for line in []:  # subprocess timed out, no output available
                    match = re.search(r'application_(\d+_\d+)', line)
                    if match:
                        application_id = f"application_{match.group(1)}"
                        job_id = application_id

                if not application_id:
                    try:
                        import subprocess as _sp
                        _cutoff = int(start_time.timestamp())
                        _r = subprocess.run(
                            ["/opt/hadoop/bin/yarn", "application", "-list", "-appStates", "RUNNING,FINISHED,FAILED,KILLED"],
                            capture_output=True, text=True, timeout=15
                        )
                        for _l in _r.stdout.splitlines():
                            _p = _l.split()
                            if len(_p) >= 4 and _p[1].isdigit():
                                _ts = int(_p[1]) // 1000
                                if abs(_ts - _cutoff) < 1200:
                                    _m = re.search(r'(application_\d+_\d+)', _l)
                                    if _m:
                                        application_id = _m.group(1)
                                        job_id = application_id
                                        break
                    except:
                        pass

                task_dir = self._create_task_dir(workload_type, batch_dir, application_id)
                logs_dir = os.path.join(task_dir, "logs")
                metrics_dir = os.path.join(task_dir, "metrics")
                os.makedirs(logs_dir, exist_ok=True)
                os.makedirs(metrics_dir, exist_ok=True)

                collect_start = (start_time - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
                collect_end = (end_time + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%S")

                log_files, log_count = self.log_collector.collect_from_loki(
                    collect_start, collect_end, logs_dir, application_id
                )
                if application_id:
                    mr_files, mr_count = self.log_collector.collect_mapreduce_logs(
                        application_id, logs_dir
                    )
                    log_files.extend(mr_files)
                    log_count += mr_count
                    try:
                        disk_f, disk_c = self.log_collector.collect_rm_nm_logs_from_disk(
                            start_time, end_time, logs_dir)
                        log_files.extend(disk_f)
                        log_count += disk_c
                    except Exception:
                        pass

                metric_files = self.metrics_collector.collect_fault_specific_metrics(
                    workload_type,
                    int((start_time - timedelta(minutes=3)).timestamp()),
                    int((end_time + timedelta(minutes=5)).timestamp()),
                    metrics_dir
                )

                exec_result = {
                    "seq_idx": seq_idx,
                    "fault_type": workload_type,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "duration": duration,
                    "success": True,
                    "job_id": job_id or "",
                    "application_id": application_id or "",
                    "logs_dir": logs_dir,
                    "metrics_dir": metrics_dir,
                    "error": "任务超时但数据已收集",
                    "nodes": "",
                    "method": f"标准{workload_type}实现(超时收集)",
                    "task_dir": task_dir
                }
                self._record_execution(batch_dir, exec_result)
                self._record_fault_label(batch_dir, exec_result)
                return exec_result
            except Exception as _e2:
                self.logger.error(f"超时后收集数据失败: {_e2}")
                task_dir = self._create_task_dir(workload_type, batch_dir)
                result = self._create_error_result(start_time, workload_type, "任务超时", seq_idx, batch_dir, task_dir)
                self._record_execution(batch_dir, result)
                return result
        except Exception as e:
            task_dir = self._create_task_dir(workload_type, batch_dir)
            result = self._create_error_result(start_time, workload_type, str(e), seq_idx, batch_dir, task_dir)
            self._record_execution(batch_dir, result)
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
        
        import select
        import time as _time
        output = []
        start_time = _time.time()
        max_timeout = 900
        
        while True:
            if _time.time() - start_time > max_timeout:
                _msg = ' timeout (' + str(max_timeout) + 's), killing process'
                print(f"\nFault script {fault_type}{_msg}")
                process.kill()
                break
            
            reads, _, _ = select.select([process.stdout], [], [], 1.0)
            if reads:
                line = process.stdout.readline()
                if not line:
                    break
                output.append(line)
                print(line, end="")
            
            if process.poll() is not None and not reads:
                remaining = process.stdout.read()
                if remaining:
                    output.append(remaining)
                    print(remaining, end="")
                break
        
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        
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
        
        # Fallback: query YARN for recently finished apps
        if not application_id:
            try:
                import subprocess as _sp
                _cutoff = int(start_time)
                _r = subprocess.run(["/opt/hadoop/bin/yarn", "application", "-list", "-appStates", "RUNNING,FINISHED,FAILED,KILLED"],
                             capture_output=True, text=True, timeout=15)
                for _l in _r.stdout.splitlines():
                    _p = _l.split()
                    if len(_p) >= 4 and _p[1].isdigit():
                        _ts = int(_p[1]) // 1000
                        if abs(_ts - _cutoff) < 1200:
                            _m = re.search(r'(application_\d+_\d+)', _l)
                            if _m:
                                application_id = _m.group(1)
                                job_id = application_id
                                break
            except:
                pass
        
        return job_id, application_id
    
    def _wait_for_task(self, application_id: str = None):
        """等待任务完成，并确保日志已写入HDFS"""
        import time
        time.sleep(10)

        if application_id:
            app_id_parts = application_id.split('_')
            if len(app_id_parts) >= 3:
                app_id_num = app_id_parts[-1]
                potential_hdfs_paths = [
                    f"/tmp/logs/ubuntu/bucket-cxw745-logs-tfile/{app_id_num}/{application_id}",
                    f"/tmp/logs/{application_id}",
                ]

                for i in range(12):
                    for hdfs_path in potential_hdfs_paths:
                        try:
                            result = subprocess.run(
                                ["/opt/hadoop/bin/hdfs", "dfs", "-test", "-d", hdfs_path],
                                capture_output=True, text=True, timeout=15
                            )
                            if result.returncode == 0:
                                self.logger.info(f"日志目录已就绪: {hdfs_path}")
                                return
                        except Exception:
                            pass
                    self.logger.info(f"等待日志目录出现... ({i+1}/12)")
                    time.sleep(5)

                self.logger.warning(f"日志目录未在预期时间内出现: {application_id}")

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
        if self.skip_prepare:
            self.logger.info("跳过数据准备 (--skip-prepare)")
            return True

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
        self.logger.info("间隔分布: Weibull(k=1.5, λ=180)")
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
            self._save_experiment_config(batch_dir, self.config.sequence)
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
                    results.append(result)
                
                if i < batch_tasks - 1 and not self._shutdown_requested:
                    import time
                    interval = _sample_interval(self.config.interval_min, self.config.interval_max)
                    self.logger.info(f"等待 {interval} 秒后执行下一次...")
                    time.sleep(interval)
            
            self._save_batch_summary(batch_dir, batch_num + 1, batch_count)
            self._save_collection_statistics(batch_dir)
        
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
                    sequence.append("normal")
        
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
    parser.add_argument("--workload", default="random", choices=["random", "wordcount", "sort", "terasort"], help="workload类型")
    parser.add_argument("--data-size", default="small",
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
    parser.add_argument("--interval-max", type=int, default=300, help="最大间隔（秒）（Weibull截断上限）")
    parser.add_argument("--max-batch-size", type=int, default=50, help="每批次最大任务数")
    parser.add_argument("--skip-prepare", action="store_true", default=False, help="跳过数据准备步骤")

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

    scheduler = UnifiedScheduler(config, args.output_dir, args.data_size, args.max_batch_size, args.workload)
    scheduler.skip_prepare = args.skip_prepare
    # Save PID for status monitoring
    try:
        with open('/tmp/scheduler_python.pid', 'w') as _f:
            _f.write(str(os.getpid()) + '\n')
        with open('/tmp/batch_scheduler.pid', 'w') as _f:
            _f.write(str(os.getpid()) + '\n')
    except:
        pass
    scheduler.run()


if __name__ == "__main__":
    main()
