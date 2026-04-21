#!/usr/bin/env python3
"""
日志收集模块

负责从Loki和HDFS NFS收集日志数据
"""
import os
import re
import csv
import subprocess
import logging
from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict

import requests


class LogCollector:
    """日志收集器"""

    def __init__(self, loki_api: str, hdfs_nfs_mount: str, logger: Optional[logging.Logger] = None):
        self.loki_api = loki_api
        self.hdfs_nfs_mount = hdfs_nfs_mount
        self.logger = logger or logging.getLogger(__name__)

    def collect_from_loki(self, start_time: str, end_time: str, output_dir: str, application_id: str = None) -> tuple[List[str], int]:
        """
        从Loki收集日志

        Args:
            start_time: 开始时间 (ISO格式)
            end_time: 结束时间 (ISO格式)
            output_dir: 输出目录
            application_id: Application ID (可选，用于过滤特定任务的日志)

        Returns:
            (文件列表, 日志条数)
        """
        self.logger.info("收集日志数据...")
        if application_id:
            self.logger.info(f"  只收集Application {application_id} 相关日志")

        def parse_time_to_ns(time_str: str) -> int:
            try:
                dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
                return int(dt.timestamp() * 1e9)
            except ValueError:
                try:
                    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    return int(dt.timestamp() * 1e9)
                except ValueError:
                    return int(datetime.now().timestamp() * 1e9)

        start_ns = parse_time_to_ns(start_time)
        end_ns = parse_time_to_ns(end_time)

        try:
            response = requests.get(f"{self.loki_api}/label/filename/values",
                                   params={"start": start_ns, "end": end_ns})
            response.raise_for_status()
            filenames = response.json().get("data", [])
        except Exception as e:
            self.logger.error(f"获取filename标签失败: {e}")
            return [], 0

        logs_by_component = defaultdict(lambda: defaultdict(list))
        task_logs = {}

        for filename in sorted(filenames):
            component = self._extract_component_from_filename(filename)
            node = self._extract_node_from_filename(filename)
            task_id = self._extract_task_id_from_filename(filename)

            try:
                params = {
                    "query": f'{{filename="{filename}"}}',
                    "start": start_ns,
                    "end": end_ns,
                    "limit": 10000,
                    "direction": "forward"
                }
                response = requests.get(f"{self.loki_api}/query_range", params=params)
                response.raise_for_status()
                results = response.json().get("data", {}).get("result", [])
            except Exception as e:
                continue

            if not results:
                continue

            self.logger.info(f"处理: {os.path.basename(filename)}")

            for stream in results:
                stream_labels = stream.get("stream", {})
                stream_level = stream_labels.get("level", "unknown")

                for timestamp_ns, line in stream.get("values", []):
                    try:
                        dt = datetime.fromtimestamp(int(timestamp_ns) / 1e9)

                        record = {
                            "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                            "timestamp_ns": timestamp_ns,
                            "component": component,
                            "node": node,
                            "filename": filename,
                            "level": stream_level,
                            "message": line
                        }

                        if task_id:
                            record["task_id"] = task_id
                            # 如果指定了application_id，只收集该任务的日志
                            if application_id:
                                if task_id in application_id or application_id.replace("application_", "") in task_id:
                                    key = f"{component}_{task_id}"
                                    if key not in task_logs:
                                        task_logs[key] = {}
                                    if node not in task_logs[key]:
                                        task_logs[key][node] = []
                                    task_logs[key][node].append(record)
                            else:
                                key = f"{component}_{task_id}"
                                if key not in task_logs:
                                    task_logs[key] = {}
                                if node not in task_logs[key]:
                                    task_logs[key][node] = []
                                task_logs[key][node].append(record)
                        else:
                            # 组件日志（namenode, resourcemanager等）总是收集
                            if component not in logs_by_component:
                                logs_by_component[component] = {}
                            if node not in logs_by_component[component]:
                                logs_by_component[component][node] = []
                            logs_by_component[component][node].append(record)
                    except Exception:
                        continue

        # 保存日志文件
        log_files = []
        total_logs = 0

        for component, logs_by_node in sorted(logs_by_component.items()):
            for node, records in sorted(logs_by_node.items()):
                if not records:
                    continue

                node_dir = os.path.join(output_dir, node)
                os.makedirs(node_dir, exist_ok=True)

                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{component}_{ts}.csv"
                filepath = os.path.join(node_dir, filename)

                with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                    fieldnames = ["timestamp", "timestamp_ns", "component", "node", "filename", "level", "message"]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
                    writer.writeheader()
                    writer.writerows(records)

                self.logger.info(f"  ✓ {node}/{filename}: {len(records)} 条")
                log_files.append(filepath)
                total_logs += len(records)

        # 合并同一application_id的所有任务日志
        # 按application_id和节点分组
        app_logs_by_node = defaultdict(lambda: defaultdict(list))
        
        for task_key, logs_by_node in sorted(task_logs.items()):
            for node, records in sorted(logs_by_node.items()):
                if not records:
                    continue
                # 从task_key提取application_id (格式: component_task_id)
                app_id = task_key.split('_')[-1] if '_' in task_key else task_key
                app_logs_by_node[app_id][node].extend(records)
        
        # 保存合并后的日志，文件名统一为 mapreduce_{application_id}.csv
        for app_id, logs_by_node in sorted(app_logs_by_node.items()):
            for node, records in sorted(logs_by_node.items()):
                if not records:
                    continue

                node_dir = os.path.join(output_dir, node)
                os.makedirs(node_dir, exist_ok=True)

                # 统一文件名格式: mapreduce_application_xxx.csv
                filename = f"mapreduce_application_{app_id}.csv"
                filepath = os.path.join(node_dir, filename)

                with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                    fieldnames = ["timestamp", "timestamp_ns", "component", "node", "filename", "level", "task_id", "message"]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
                    writer.writeheader()
                    writer.writerows(records)

                self.logger.info(f"  ✓ {node}/{filename}: {len(records)} 条")
                log_files.append(filepath)
                total_logs += len(records)

        self.logger.info(f"从Loki收集到 {total_logs} 条日志，保存到 {len(log_files)} 个文件")
        return log_files, total_logs

    def collect_mapreduce_logs(self, application_id: str, output_dir: str) -> tuple[List[str], int]:
        """
        从HDFS NFS收集MapReduce任务日志
        
        每个application_id对应一个日志目录，目录下有多个节点的日志文件（如cpf-2_40553）
        每个节点只生成一个CSV文件，文件名统一为 mapreduce_{application_id}.csv

        Args:
            application_id: Application ID
            output_dir: 输出目录

        Returns:
            (文件列表, 日志条数)
        """
        if not application_id:
            self.logger.info("没有Application ID，跳过MapReduce日志收集")
            return [], 0

        self.logger.info(f"收集MapReduce任务日志 (Application: {application_id})...")

        mr_logs_base = os.path.join(self.hdfs_nfs_mount, "tmp/logs")
        if not os.path.exists(mr_logs_base):
            self.logger.error(f"MapReduce日志目录不存在: {mr_logs_base}")
            return [], 0

        # 直接从application_id提取ID号，构造路径
        # application_id格式: application_1774003847013_0012
        app_id_parts = application_id.split('_')
        if len(app_id_parts) >= 3:
            app_id_num = app_id_parts[-1]  # 获取最后一部分，如 "0012"
            # 尝试直接构造路径，避免遍历整个目录树
            potential_paths = [
                os.path.join(mr_logs_base, "ubuntu", "bucket-cxw745-logs-tfile", app_id_num, application_id),
                os.path.join(mr_logs_base, application_id),
            ]
            
            app_log_dir = None
            for path in potential_paths:
                if os.path.exists(path):
                    app_log_dir = path
                    self.logger.info(f"直接找到日志目录: {app_log_dir}")
                    break
            
            # 如果直接构造路径没找到，再使用遍历
            if not app_log_dir:
                self.logger.info(f"直接路径未找到，开始遍历查找: {application_id}")
                for root, dirs, files in os.walk(mr_logs_base):
                    if application_id in dirs:
                        app_log_dir = os.path.join(root, application_id)
                        break
        else:
            # 如果application_id格式不对，使用遍历
            for root, dirs, files in os.walk(mr_logs_base):
                if application_id in dirs:
                    app_log_dir = os.path.join(root, application_id)
                    break

        if not app_log_dir or not os.path.exists(app_log_dir):
            self.logger.info(f"未找到Application日志目录: {application_id}")
            return [], 0

        self.logger.info(f"找到日志目录: {app_log_dir}")

        log_files = []
        total_logs = 0

        node_logs = {}

        for log_file in os.listdir(app_log_dir):
            log_path = os.path.join(app_log_dir, log_file)
            if not os.path.isfile(log_path):
                continue

            match = re.match(r'(cpf-\d+)_(\d+)', log_file)
            if not match:
                self.logger.info(f"跳过不匹配的文件: {log_file}")
                continue

            node = match.group(1)
            port = match.group(2)

            self.logger.info(f"处理: {log_file} -> 节点: {node}")

            if node not in node_logs:
                node_logs[node] = []

            try:
                result = subprocess.run(
                    ["hadoop", "fs", "-text", f"/tmp/logs/{os.path.relpath(log_path, mr_logs_base)}"],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    errors='replace'
                )

                if result.returncode != 0:
                    self.logger.error(f"解析TFile失败: {log_file}")
                    continue

                content = result.stdout
                if not content:
                    continue

                records = self._parse_tfile_content(content, node, application_id)
                node_logs[node].extend(records)

            except subprocess.TimeoutExpired:
                self.logger.error(f"解析TFile超时: {log_file}")
            except Exception as e:
                self.logger.error(f"处理日志文件失败 {log_file}: {e}")

        for node, records in node_logs.items():
            if not records:
                continue

            node_dir = os.path.join(output_dir, node)
            os.makedirs(node_dir, exist_ok=True)

            csv_file = os.path.join(node_dir, f"mapreduce_{application_id}.csv")

            with open(csv_file, "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = ["timestamp", "log_type", "container_id", "container_timestamp", "node", "application_id", "message"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
                writer.writeheader()
                writer.writerows(records)

            self.logger.info(f"  ✓ {node}/mapreduce_{application_id}.csv: {len(records)} 条")
            log_files.append(csv_file)
            total_logs += len(records)

        self.logger.info(f"MapReduce日志收集完成: {total_logs} 条，{len(log_files)} 个文件")
        return log_files, total_logs

    def _parse_tfile_content(self, content: str, node: str, application_id: str) -> List[Dict]:
        """解析TFile内容"""
        records = []
        current_log_type = None
        current_container = None
        current_content = []

        log_types = ['syslog', 'stdout', 'stderr', 'syslog.shuffle', 'prelaunch.err',
                     'prelaunch.out', 'directory.info', 'launch_container.sh']

        for line in content.split('\n'):
            stripped = line.strip()
            found_type = None

            for log_type in log_types:
                if stripped == log_type or stripped.startswith(log_type):
                    found_type = log_type
                    break

            if found_type:
                if current_log_type and current_content:
                    for msg_line in current_content:
                        if msg_line.strip():
                            timestamp = self._extract_timestamp(msg_line)
                            container_timestamp = self._extract_container_timestamp(current_container)
                            records.append({
                                "timestamp": timestamp,
                                "log_type": current_log_type,
                                "container_id": current_container or "unknown",
                                "container_timestamp": container_timestamp,
                                "node": node,
                                "application_id": application_id,
                                "message": msg_line.strip()
                            })

                current_log_type = found_type
                current_content = []
            elif re.match(r'container_\d+_\d+_\d+_\d+', stripped):
                current_container = stripped
            elif stripped.isdigit() and len(stripped) <= 10:
                continue
            else:
                current_content.append(line)

        if current_log_type and current_content:
            for msg_line in current_content:
                if msg_line.strip():
                    timestamp = self._extract_timestamp(msg_line)
                    container_timestamp = self._extract_container_timestamp(current_container)
                    records.append({
                        "timestamp": timestamp,
                        "log_type": current_log_type,
                        "container_id": current_container or "unknown",
                        "container_timestamp": container_timestamp,
                        "node": node,
                        "application_id": application_id,
                        "message": msg_line.strip()
                    })

        return records
    
    def _extract_container_timestamp(self, container_id: str) -> str:
        """
        从container_id提取时间戳
        
        container_id格式: container_1772798254657_0286_20260319_163759
        最后的_后面是时间戳: 20260319_163759 -> 2026-03-19 16:37:59
        """
        if not container_id:
            return ""
        
        match = re.search(r'(\d{8})_(\d{6})$', container_id)
        if match:
            date_str = match.group(1)
            time_str = match.group(2)
            try:
                formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} {time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
                return formatted
            except Exception:
                return f"{date_str}_{time_str}"
        
        return ""

    def _extract_timestamp(self, line: str) -> str:
        """从日志行提取时间戳"""
        patterns = [
            r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})',
            r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})',
            r'(\d{2}:\d{2}:\d{2})',
        ]

        for pattern in patterns:
            match = re.search(pattern, line)
            if match:
                return match.group(1)

        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _extract_component_from_filename(self, filename: str) -> str:
        """从文件名提取组件名"""
        if not filename:
            return "unknown"

        if "datanode" in filename:
            return "datanode"
        elif "namenode" in filename:
            if "secondary" in filename:
                return "secondarynamenode"
            return "namenode"
        elif "nodemanager" in filename:
            return "nodemanager"
        elif "resourcemanager" in filename:
            return "resourcemanager"
        else:
            return "mapreduce"

    def _extract_node_from_filename(self, filename: str) -> str:
        """从文件名提取节点名"""
        if not filename:
            return "unknown"

        patterns = {
            "datanode": r"datanode-(cpf-\d+)",
            "namenode": r"namenode-(cpf-\d+)",
            "nodemanager": r"nodemanager-(cpf-\d+)",
            "resourcemanager": r"resourcemanager-(cpf-\d+)",
            "secondarynamenode": r"secondarynamenode-(cpf-\d+)",
            "mapreduce_task": r"cpf-(\d+)_(\d+)"
        }

        for pattern_name, pattern in patterns.items():
            match = re.search(pattern, filename)
            if match:
                if pattern_name == "mapreduce_task":
                    return f"cpf-{match.group(1)}"
                else:
                    return match.group(1)

        return "unknown"

    def _extract_task_id_from_filename(self, filename: str) -> Optional[str]:
        """从文件名提取任务ID"""
        if not filename:
            return None

        match = re.search(r"application_(\d+_\d+)", filename)
        if match:
            return match.group(1)

        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    collector = LogCollector(
        loki_api="http://localhost:3100/loki/api/v1",
        hdfs_nfs_mount="/hdfs-nfs"
    )

    print("LogCollector initialized")
