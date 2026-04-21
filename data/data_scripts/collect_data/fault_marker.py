#!/usr/bin/env python3
"""
故障标记模块 V2

用于在故障注入时添加标记日志，方便验证故障是否成功注入。
支持统一配置和不同故障类型的自定义标记。
"""
import os
import time
import json
from datetime import datetime
from typing import Dict, Optional, Any

# 默认配置
DEFAULT_CONFIG = {
    "enabled": False,
    "log_path": "/tmp/fault_markers.log",
    "default_start_marker": "FAULT_INJECTION_START",
    "default_end_marker": "FAULT_INJECTION_END",
    "include_timestamp": True,
    "include_unix_time": True,
    "print_to_console": True
}

# 故障类型特定标记配置
FAULT_MARKER_CONFIG = {
    "data_skew": {
        "enabled": True,
        "start_marker": "DATA_SKEW_INJECTION_START",
        "end_marker": "DATA_SKEW_INJECTION_END",
        "description": "数据倾斜故障注入"
    },
    "data_bloat": {
        "enabled": True,
        "start_marker": "DATA_BLOAT_INJECTION_START",
        "end_marker": "DATA_BLOAT_INJECTION_END",
        "description": "数据膨胀故障注入"
    },
    "task_fail": {
        "enabled": True,
        "start_marker": "TASK_FAIL_INJECTION_START",
        "end_marker": "TASK_FAIL_INJECTION_END",
        "description": "任务失败故障注入"
    },
    "long_tail": {
        "enabled": True,
        "start_marker": "LONG_TAIL_INJECTION_START",
        "end_marker": "LONG_TAIL_INJECTION_END",
        "description": "长尾任务故障注入"
    },
    "wait_time": {
        "enabled": True,
        "start_marker": "WAIT_TIME_INJECTION_START",
        "end_marker": "WAIT_TIME_INJECTION_END",
        "description": "等待时间异常注入"
    },
    "runtime_delta": {
        "enabled": True,
        "start_marker": "RUNTIME_DELTA_INJECTION_START",
        "end_marker": "RUNTIME_DELTA_INJECTION_END",
        "description": "运行时间异常注入"
    },
    "exit_time": {
        "enabled": True,
        "start_marker": "EXIT_TIME_INJECTION_START",
        "end_marker": "EXIT_TIME_INJECTION_END",
        "description": "退出时间异常注入"
    },
    "network_latency": {
        "enabled": True,
        "start_marker": "NETWORK_LATENCY_INJECTION_START",
        "end_marker": "NETWORK_LATENCY_INJECTION_END",
        "description": "网络延迟故障注入"
    },
    "normal": {
        "enabled": False,  # 默认不标记normal任务
        "start_marker": "NORMAL_TASK_START",
        "end_marker": "NORMAL_TASK_END",
        "description": "正常任务执行"
    }
}


class FaultMarker:
    """故障标记器"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化故障标记器
        
        Args:
            config: 全局配置，覆盖默认配置
        """
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.fault_configs = FAULT_MARKER_CONFIG.copy()
        
        # 从环境变量读取全局启用状态
        env_enabled = os.environ.get("FAULT_MARKER_ENABLED", "").lower()
        if env_enabled in ("true", "1", "yes"):
            self.config["enabled"] = True
        elif env_enabled in ("false", "0", "no"):
            self.config["enabled"] = False
        
        # 从环境变量读取日志路径
        env_log_path = os.environ.get("FAULT_MARKER_LOG_PATH")
        if env_log_path:
            self.config["log_path"] = env_log_path
    
    def set_global_config(self, **kwargs):
        """
        设置全局配置
        
        Args:
            enabled: 是否启用标记
            log_path: 日志文件路径
            default_start_marker: 默认开始标记
            default_end_marker: 默认结束标记
            include_timestamp: 是否包含时间戳
            include_unix_time: 是否包含Unix时间
            print_to_console: 是否打印到控制台
        """
        self.config.update(kwargs)
    
    def set_fault_config(self, fault_type: str, **kwargs):
        """
        设置特定故障类型的标记配置
        
        Args:
            fault_type: 故障类型
            enabled: 是否启用该故障类型的标记
            start_marker: 开始标记内容
            end_marker: 结束标记内容
            description: 故障描述
        """
        if fault_type not in self.fault_configs:
            self.fault_configs[fault_type] = {}
        self.fault_configs[fault_type].update(kwargs)
    
    def is_enabled(self, fault_type: str = None) -> bool:
        """
        检查标记功能是否启用
        
        Args:
            fault_type: 故障类型，如果指定则检查该类型的启用状态
            
        Returns:
            bool: 是否启用
        """
        if not self.config["enabled"]:
            return False
        
        if fault_type and fault_type in self.fault_configs:
            return self.fault_configs[fault_type].get("enabled", True)
        
        return True
    
    def mark_start(self, fault_type: str, details: Dict = None):
        """
        标记故障开始
        
        Args:
            fault_type: 故障类型
            details: 额外的详细信息
        """
        if not self.is_enabled(fault_type):
            return
        
        fault_config = self.fault_configs.get(fault_type, {})
        marker_text = fault_config.get("start_marker", self.config["default_start_marker"])
        description = fault_config.get("description", fault_type)
        
        marker = {
            "event": marker_text,
            "fault_type": fault_type,
            "description": description,
            "phase": "START"
        }
        
        if self.config["include_timestamp"]:
            marker["timestamp"] = datetime.now().isoformat()
        
        if self.config["include_unix_time"]:
            marker["unix_time"] = time.time()
        
        if details:
            marker["details"] = details
        
        self._write_marker(marker)
        
        if self.config["print_to_console"]:
            print(f"[FAULT_MARKER] {marker_text} - {description}")
    
    def mark_end(self, fault_type: str, details: Dict = None):
        """
        标记故障结束
        
        Args:
            fault_type: 故障类型
            details: 额外的详细信息
        """
        if not self.is_enabled(fault_type):
            return
        
        fault_config = self.fault_configs.get(fault_type, {})
        marker_text = fault_config.get("end_marker", self.config["default_end_marker"])
        description = fault_config.get("description", fault_type)
        
        marker = {
            "event": marker_text,
            "fault_type": fault_type,
            "description": description,
            "phase": "END"
        }
        
        if self.config["include_timestamp"]:
            marker["timestamp"] = datetime.now().isoformat()
        
        if self.config["include_unix_time"]:
            marker["unix_time"] = time.time()
        
        if details:
            marker["details"] = details
        
        self._write_marker(marker)
        
        if self.config["print_to_console"]:
            print(f"[FAULT_MARKER] {marker_text} - {description}")
    
    def mark_injection(self, fault_type: str, action: str, target: str = None, duration: float = None):
        """
        标记故障注入动作
        
        Args:
            fault_type: 故障类型
            action: 注入动作描述
            target: 目标（节点、进程等）
            duration: 持续时间（秒）
        """
        if not self.is_enabled(fault_type):
            return
        
        fault_config = self.fault_configs.get(fault_type, {})
        description = fault_config.get("description", fault_type)
        
        marker = {
            "event": "FAULT_INJECTION_ACTION",
            "fault_type": fault_type,
            "description": description,
            "action": action
        }
        
        if self.config["include_timestamp"]:
            marker["timestamp"] = datetime.now().isoformat()
        
        if self.config["include_unix_time"]:
            marker["unix_time"] = time.time()
        
        if target:
            marker["target"] = target
        
        if duration:
            marker["duration"] = duration
        
        self._write_marker(marker)
        
        if self.config["print_to_console"]:
            target_str = f" -> {target}" if target else ""
            duration_str = f" ({duration}s)" if duration else ""
            print(f"[FAULT_MARKER] {fault_type}{target_str}: {action}{duration_str}")
    
    def _write_marker(self, marker: Dict):
        """写入标记到日志文件"""
        try:
            log_path = self.config["log_path"]
            # 确保目录存在
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            
            with open(log_path, "a") as f:
                f.write(json.dumps(marker, ensure_ascii=False) + "\n")
        except Exception as e:
            if self.config["print_to_console"]:
                print(f"[FAULT_MARKER] 写入标记失败: {e}")
    
    def get_markers(self, fault_type: str = None, phase: str = None, 
                   start_time: float = None, end_time: float = None) -> list:
        """
        获取故障标记记录
        
        Args:
            fault_type: 筛选特定故障类型
            phase: 筛选阶段 (START/END)
            start_time: 起始时间戳
            end_time: 结束时间戳
            
        Returns:
            list: 符合条件的标记列表
        """
        markers = []
        try:
            with open(self.config["log_path"], "r") as f:
                for line in f:
                    try:
                        marker = json.loads(line.strip())
                        
                        if fault_type and marker.get("fault_type") != fault_type:
                            continue
                        
                        if phase and marker.get("phase") != phase:
                            continue
                        
                        if start_time and marker.get("unix_time", 0) < start_time:
                            continue
                        
                        if end_time and marker.get("unix_time", 0) > end_time:
                            continue
                        
                        markers.append(marker)
                    except json.JSONDecodeError:
                        continue
        except FileNotFoundError:
            pass
        return markers
    
    def clear_markers(self):
        """清除所有标记"""
        try:
            if os.path.exists(self.config["log_path"]):
                os.remove(self.config["log_path"])
        except Exception as e:
            if self.config["print_to_console"]:
                print(f"[FAULT_MARKER] 清除标记失败: {e}")
    
    def get_statistics(self, fault_type: str = None) -> Dict:
        """
        获取标记统计信息
        
        Args:
            fault_type: 特定故障类型，不指定则统计全部
            
        Returns:
            dict: 统计信息
        """
        markers = self.get_markers(fault_type=fault_type)
        
        stats = {
            "total_markers": len(markers),
            "start_markers": len([m for m in markers if m.get("phase") == "START"]),
            "end_markers": len([m for m in markers if m.get("phase") == "END"]),
            "action_markers": len([m for m in markers if m.get("event") == "FAULT_INJECTION_ACTION"]),
            "by_fault_type": {}
        }
        
        for marker in markers:
            ft = marker.get("fault_type", "unknown")
            if ft not in stats["by_fault_type"]:
                stats["by_fault_type"][ft] = {"start": 0, "end": 0, "action": 0}
            
            if marker.get("phase") == "START":
                stats["by_fault_type"][ft]["start"] += 1
            elif marker.get("phase") == "END":
                stats["by_fault_type"][ft]["end"] += 1
            elif marker.get("event") == "FAULT_INJECTION_ACTION":
                stats["by_fault_type"][ft]["action"] += 1
        
        return stats


# 全局标记器实例（单例模式）
_global_marker = None


def get_marker(config: Dict = None) -> FaultMarker:
    """
    获取全局标记器实例
    
    Args:
        config: 配置字典，首次调用时生效
        
    Returns:
        FaultMarker: 标记器实例
    """
    global _global_marker
    if _global_marker is None:
        _global_marker = FaultMarker(config)
    return _global_marker


# 兼容旧版API的便捷函数
def mark_fault_start(fault_type: str, details: Dict = None):
    """标记故障开始（兼容旧版）"""
    marker = get_marker()
    marker.mark_start(fault_type, details)


def mark_fault_end(fault_type: str, details: Dict = None):
    """标记故障结束（兼容旧版）"""
    marker = get_marker()
    marker.mark_end(fault_type, details)


def mark_fault_injection(fault_type: str, target: str, action: str, duration: float = None):
    """标记故障注入动作（兼容旧版）"""
    marker = get_marker()
    marker.mark_injection(fault_type, action, target, duration)


def is_marker_enabled(fault_type: str = None) -> bool:
    """检查标记功能是否启用（兼容旧版）"""
    marker = get_marker()
    return marker.is_enabled(fault_type)


if __name__ == "__main__":
    # 测试示例
    print("=" * 60)
    print("故障标记模块测试")
    print("=" * 60)
    
    # 创建标记器并启用
    marker = FaultMarker({"enabled": True, "log_path": "/tmp/test_fault_markers.log"})
    
    # 测试不同故障类型的标记
    print("\n1. 测试 data_skew 故障标记:")
    marker.mark_start("data_skew", {"skew_ratio": 0.8})
    marker.mark_injection("data_skew", "inject_skew_key", "Mapper", 120)
    marker.mark_end("data_skew", {"result": "success"})
    
    print("\n2. 测试 task_fail 故障标记:")
    marker.mark_start("task_fail", {"fail_rate": 0.2})
    marker.mark_injection("task_fail", "throw_exception", "Mapper", None)
    marker.mark_end("task_fail")
    
    print("\n3. 测试 wait_time 故障标记:")
    marker.mark_start("wait_time", {"duration": 120})
    marker.mark_injection("wait_time", "SIGSTOP", "ResourceManager", 120)
    marker.mark_end("wait_time")
    
    # 查看统计
    print("\n4. 标记统计:")
    stats = marker.get_statistics()
    print(f"  总标记数: {stats['total_markers']}")
    print(f"  开始标记: {stats['start_markers']}")
    print(f"  结束标记: {stats['end_markers']}")
    print(f"  动作标记: {stats['action_markers']}")
    print(f"  按故障类型: {stats['by_fault_type']}")
    
    # 查看标记文件
    print(f"\n5. 标记文件内容 ({marker.config['log_path']}):")
    try:
        with open(marker.config["log_path"], "r") as f:
            for i, line in enumerate(f, 1):
                print(f"  {i}. {line.strip()}")
    except FileNotFoundError:
        print("  文件不存在")
    
    # 清理测试文件
    marker.clear_markers()
    print("\n6. 测试完成，已清理标记文件")
