#!/usr/bin/env python3
"""
运行记录模块

负责记录每次故障注入的执行信息
"""
import os
import json
import csv
import logging
from datetime import datetime
from typing import List, Dict, Optional


class RunRecorder:
    """运行记录器"""
    
    def __init__(self, output_dir: str, logger: Optional[logging.Logger] = None):
        self.output_dir = output_dir
        self.logger = logger or logging.getLogger(__name__)
        self.records = []
        self.csv_file = os.path.join(output_dir, "execution_records.csv")
        self.json_file = os.path.join(output_dir, "execution_summary.json")
        
        os.makedirs(output_dir, exist_ok=True)
        self._init_csv()
    
    def _init_csv(self):
        """初始化CSV文件"""
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, "w", newline="", encoding="utf-8") as f:
                fieldnames = [
                    "sequence_index", "fault_type", "start_time", "end_time",
                    "duration_seconds", "success", "job_id", "application_id",
                    "logs_collected", "metrics_collected", "error_message",
                    "affected_nodes", "injection_method", "task_dir"
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
    
    def record(self, record: Dict):
        """
        记录一次执行
        
        Args:
            record: 执行记录字典，包含以下字段:
                - sequence_index: 序列索引
                - fault_type: 故障类型
                - start_time: 开始时间
                - end_time: 结束时间
                - duration_seconds: 执行时长
                - success: 是否成功
                - job_id: Job ID
                - application_id: Application ID
                - logs_collected: 收集的日志数量
                - metrics_collected: 收集的指标数量
                - error_message: 错误信息
                - affected_nodes: 受影响的节点
                - injection_method: 注入方法
                - task_dir: 任务目录 (可选)
        """
        self.records.append(record)
        
        # 定义CSV字段
        fieldnames = [
            "sequence_index", "fault_type", "start_time", "end_time",
            "duration_seconds", "success", "job_id", "application_id",
            "logs_collected", "metrics_collected", "error_message",
            "affected_nodes", "injection_method", "task_dir"
        ]
        
        # 过滤记录，只保留CSV字段
        csv_record = {k: record.get(k, "") for k in fieldnames}
        
        # 追加到CSV
        with open(self.csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(csv_record)
        
        self.logger.debug(f"记录执行: {record.get('fault_type')} - {'成功' if record.get('success') else '失败'}")
    
    def save_summary(self, config: Dict, statistics: Dict):
        """
        保存执行摘要
        
        Args:
            config: 配置信息
            statistics: 统计信息
        """
        summary = {
            "start_time": datetime.now().isoformat(),
            "total_runs": len(self.records),
            "successful_runs": sum(1 for r in self.records if r.get("success", False)),
            "failed_runs": sum(1 for r in self.records if not r.get("success", False)),
            "config": config,
            "statistics": statistics,
            "records": self.records
        }
        
        with open(self.json_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"执行摘要已保存: {self.json_file}")
    
    def get_records(self) -> List[Dict]:
        """获取所有记录"""
        return self.records.copy()
    
    def get_statistics(self) -> Dict:
        """获取统计信息"""
        if not self.records:
            return {}
        
        fault_types = {}
        for r in self.records:
            ft = r.get("fault_type", "unknown")
            if ft not in fault_types:
                fault_types[ft] = {"count": 0, "success": 0, "failed": 0}
            fault_types[ft]["count"] += 1
            if r.get("success", False):
                fault_types[ft]["success"] += 1
            else:
                fault_types[ft]["failed"] += 1
        
        return {
            "total_runs": len(self.records),
            "successful_runs": sum(1 for r in self.records if r.get("success", False)),
            "failed_runs": sum(1 for r in self.records if not r.get("success", False)),
            "fault_type_distribution": fault_types,
            "total_logs_collected": sum(r.get("logs_collected", 0) for r in self.records),
            "total_metrics_collected": sum(r.get("metrics_collected", 0) for r in self.records)
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    recorder = RunRecorder("/tmp/test_records")
    
    # 测试记录
    recorder.record({
        "sequence_index": 1,
        "fault_type": "data_skew",
        "start_time": datetime.now().isoformat(),
        "end_time": datetime.now().isoformat(),
        "duration_seconds": 120,
        "success": True,
        "job_id": "job_123",
        "application_id": "app_123",
        "logs_collected": 1000,
        "metrics_collected": 500,
        "error_message": "",
        "affected_nodes": "cxw-2,cxw-3",
        "injection_method": "Mapper代码"
    })
    
    print("Statistics:", recorder.get_statistics())
