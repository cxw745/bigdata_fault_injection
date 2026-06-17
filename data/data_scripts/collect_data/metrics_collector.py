#!/usr/bin/env python3
"""
指标收集模块

用于收集Prometheus指标，支持按故障类型收集相关指标。
"""
import requests
import time
import json
import csv
import os
import logging
from datetime import datetime
from unified_config import METRICS_CONFIG, PROMETHEUS_API, INSTANCE_TO_HOSTNAME, SERVICE_PORTS


class MetricsCollector:
    def __init__(self, prometheus_url=None, logger=None):
        self.prometheus_api = prometheus_url or PROMETHEUS_API
        self.session = requests.Session()
        self.logger = logger or logging.getLogger(__name__)

    def query_prometheus(self, query, start_time=None, end_time=None, step="15s"):
        """
        查询Prometheus指标

        Args:
            query: PromQL查询语句
            start_time: 起始时间戳
            end_time: 结束时间戳
            step: 查询步长

        Returns:
            dict: 查询结果
        """
        try:
            if start_time and end_time:
                # 范围查询
                url = f"{self.prometheus_api}/query_range"
                params = {
                    "query": query,
                    "start": start_time,
                    "end": end_time,
                    "step": step
                }
            else:
                # 瞬时查询
                url = f"{self.prometheus_api}/query"
                params = {"query": query, "time": end_time or time.time()}

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"[MetricsCollector] 查询失败: {e}")
            return {"status": "error", "error": str(e)}

    def collect_metrics_by_category(self, category, start_time=None, end_time=None, output_dir=None):
        """
        按类别收集指标并保存为CSV

        Args:
            category: 指标类别 (cpu/memory/disk/network/hadoop/jvm)
            start_time: 起始时间戳
            end_time: 结束时间戳
            output_dir: 输出目录

        Returns:
            list: 保存的文件列表
        """
        if category not in METRICS_CONFIG:
            self.logger.warning(f"未知指标类别: {category}")
            return []

        self.logger.info(f"收集 {category} 指标...")
        metrics_list = METRICS_CONFIG[category]
        all_records = []

        for metric_query in metrics_list:
            try:
                result = self.query_prometheus(metric_query, start_time, end_time)

                if result.get("status") != "success":
                    continue

                results = result.get("data", {}).get("result", [])

                for res in results:
                    metric_labels = res.get("metric", {})
                    instance = metric_labels.get("instance", "")
                    hostname = INSTANCE_TO_HOSTNAME.get(instance, instance.split(":")[0] if ":" in instance else "unknown")

                    if hostname == "unknown" or not hostname.startswith("cxw-"):
                        continue

                    service = SERVICE_PORTS.get(instance.split(":")[1] if ":" in instance else "", "unknown")
                    metric_name = metric_query.split("{")[0]

                    values = res.get("values", [])
                    if not values and res.get("value"):
                        values = [res["value"]]

                    for timestamp, value in values:
                        try:
                            dt = datetime.fromtimestamp(timestamp)
                            record = {
                                "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                                "timestamp_unix": timestamp,
                                "metric": metric_name,
                                "category": category,
                                "hostname": hostname,
                                "service": service,
                                "device": metric_labels.get("device", ""),
                                "mode": metric_labels.get("mode", ""),
                                "mountpoint": metric_labels.get("mountpoint", ""),
                                "value": float(value)
                            }
                            all_records.append(record)
                        except Exception:
                            continue

            except Exception as e:
                self.logger.error(f"  ✗ {metric_query}: {e}")
                continue

        # 保存为CSV文件
        saved_files = []
        if output_dir and all_records:
            # 按主机和指标分组
            records_by_host_and_metric = {}
            for record in all_records:
                hostname = record.get("hostname", "unknown")
                metric = record.get("metric", "unknown")
                key = f"{hostname}/{metric}"
                if key not in records_by_host_and_metric:
                    records_by_host_and_metric[key] = []
                records_by_host_and_metric[key].append(record)

            for key, records in sorted(records_by_host_and_metric.items()):
                if not records:
                    continue

                hostname, metric = key.split("/")
                node_dir = os.path.join(output_dir, hostname)
                os.makedirs(node_dir, exist_ok=True)

                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{metric}_{ts}.csv"
                filepath = os.path.join(node_dir, filename)

                fieldnames = ["timestamp", "timestamp_unix", "metric", "category", "hostname", "service", "device", "mode", "mountpoint", "value"]
                with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
                    writer.writeheader()
                    writer.writerows(records)

                self.logger.info(f"  ✓ {hostname}/{filename}: {len(records)} 条")
                saved_files.append(filepath)

        return saved_files

    def collect_all_metrics(self, start_time=None, end_time=None, output_dir=None):
        """
        收集所有配置的指标

        Args:
            start_time: 起始时间戳
            end_time: 结束时间戳
            output_dir: 输出目录

        Returns:
            list: 所有保存的文件列表
        """
        all_files = []
        for category in list(METRICS_CONFIG.keys()):
            files = self.collect_metrics_by_category(category, start_time, end_time, output_dir)
            all_files.extend(files)
        return all_files

    def collect_core_metrics(self, start_time=None, end_time=None, output_dir=None):
        """
        只收集核心指标（用于故障检测）

        Args:
            start_time: 起始时间戳
            end_time: 结束时间戳
            output_dir: 输出目录

        Returns:
            list: 保存的文件列表
        """
        from unified_config import CORE_METRICS
        all_files = []
        self.logger.info("收集核心指标 (5个)...")

        for metric_name, metric_query in CORE_METRICS.items():
            try:
                result = self.query_prometheus(metric_query, start_time, end_time)

                if result.get("status") != "success":
                    self.logger.warning(f"  {metric_name}: 查询失败")
                    continue

                results = result.get("data", {}).get("result", [])
                if not results:
                    self.logger.warning(f"  {metric_name}: 无数据")
                    continue

                os.makedirs(output_dir, exist_ok=True)
                output_file = os.path.join(output_dir, f"{metric_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

                with open(output_file, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["timestamp", "timestamp_unix", "hostname", "instance", "value"])

                    for res in results:
                        metric_labels = res.get("metric", {})
                        instance = metric_labels.get("instance", "")
                        hostname = INSTANCE_TO_HOSTNAME.get(instance, instance.split(":")[0] if ":" in instance else "unknown")

                        if hostname == "unknown" or not hostname.startswith("cxw-"):
                            continue

                        values = res.get("values", [])
                        if not values and res.get("value"):
                            values = [res["value"]]

                        for timestamp, value in values:
                            try:
                                dt = datetime.fromtimestamp(timestamp)
                                writer.writerow([
                                    dt.strftime("%Y-%m-%d %H:%M:%S"),
                                    timestamp,
                                    hostname,
                                    instance,
                                    float(value)
                                ])
                            except Exception:
                                continue

                record_count = sum(1 for _ in open(output_file)) - 1
                self.logger.info(f"  ✓ {metric_name}: {record_count} 条数据")
                all_files.append(output_file)

            except Exception as e:
                self.logger.error(f"  ✗ {metric_name}: {e}")

        return all_files

    def collect_fault_specific_metrics(self, fault_type, start_time=None, end_time=None, output_dir=None):
        """
        根据故障类型收集相关指标

        Args:
            fault_type: 故障类型
            start_time: 起始时间戳
            end_time: 结束时间戳
            output_dir: 输出目录

        Returns:
            list: 保存的文件列表
        """
        # 故障类型到指标类别的映射
        fault_metrics_mapping = {
            "wordcount": ["hadoop", "cpu", "memory", "jvm", "disk", "network"],
            "data_skew": ["hadoop", "jvm", "cpu", "memory"],
            "data_bloat": ["hadoop", "disk", "network", "jvm"],
            "task_fail": ["hadoop", "jvm", "memory"],
            "long_tail": ["hadoop", "cpu", "jvm", "memory"],
            "wait_time": ["hadoop", "cpu", "jvm"],
            "runtime_delta": ["hadoop", "jvm", "cpu"],
            "exit_time": ["hadoop", "jvm", "memory"],
            "network_latency": ["network", "hadoop", "cpu"]
        }

        categories = list(METRICS_CONFIG.keys())

        all_files = []
        for category in categories:
            files = self.collect_metrics_by_category(category, start_time, end_time, output_dir)
            all_files.extend(files)

        return all_files

    def save_metrics(self, metrics_data, output_path):
        """保存指标数据到文件"""
        try:
            with open(output_path, "w") as f:
                json.dump(metrics_data, f, indent=2, default=str)
            self.logger.info(f"[MetricsCollector] 指标已保存到: {output_path}")
        except Exception as e:
            self.logger.error(f"[MetricsCollector] 保存失败: {e}")


if __name__ == "__main__":
    # 测试收集
    logging.basicConfig(level=logging.INFO)
    collector = MetricsCollector()
    print("测试收集指标...")
    files = collector.collect_all_metrics()
    print(f"收集完成，文件数: {len(files)}")
