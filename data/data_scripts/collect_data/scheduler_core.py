#!/usr/bin/env python3
"""
故障调度核心模块

负责故障注入的调度逻辑，包括：
- 故障序列生成（顺序/随机）
- 故障比例控制（故障/正常比例）
- 执行间隔控制（随机范围）
- 停止条件检查（日志数量等）
"""
import random
import time
import logging
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum


class ScheduleMode(Enum):
    """调度模式"""
    SEQUENTIAL = "sequential"  # 顺序模式：按指定顺序执行
    RANDOM = "random"          # 随机模式：随机顺序执行


@dataclass
class SchedulerConfig:
    """调度器配置"""
    # 调度模式
    mode: ScheduleMode = ScheduleMode.SEQUENTIAL  # 默认顺序模式
    
    # 故障序列配置 - 顺序模式
    # 格式: [("fault_type", count), ("normal", count), ...]
    # 例如: [("data_skew", 3), ("task_fail", 2), ("normal", 2)]
    sequence: List[tuple] = field(default_factory=list)
    
    # 故障序列配置 - 随机模式
    fault_types: List[str] = field(default_factory=list)  # 可用的故障类型列表
    fault_counts: Dict[str, int] = field(default_factory=dict)  # 每种故障的数量
    total_count: int = 0  # 总执行次数（如果为0，则根据fault_counts计算）
    include_normal: bool = True  # 是否包含无故障运行
    normal_count: int = 0  # normal任务数量（随机模式下）
    
    # 执行顺序配置
    random_order: bool = False  # 是否随机顺序（在RANDOM模式下自动为True）
    
    # 时间配置 - 随机间隔范围
    interval_min: int = 60  # 最小间隔（秒）
    interval_max: int = 300  # 最大间隔（秒）
    
    # 停止条件
    max_logs: Optional[int] = None  # 收集到多少条日志停止
    max_duration: Optional[int] = None  # 最大运行时长（秒）
    
    # 故障参数
    default_fault_duration: int = 120  # 默认故障持续时间


class FaultScheduler:
    """故障调度器"""

    def __init__(self, config: SchedulerConfig, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.execution_count = 0
        self.total_logs_collected = 0
        self.start_time = time.time()
        self.fault_execution_count = {}
        self.remaining_sequence = []  # 剩余待执行的序列
        
        # 初始化计数器
        if config.mode == ScheduleMode.SEQUENTIAL and config.sequence:
            for fault_type, _ in config.sequence:
                self.fault_execution_count[fault_type] = 0
        else:
            for fault_type in config.fault_types:
                self.fault_execution_count[fault_type] = 0
            if config.include_normal:
                self.fault_execution_count["normal"] = 0

    def generate_sequence(self) -> List[str]:
        """
        生成故障执行序列
        
        Returns:
            故障类型列表
        """
        if self.config.mode == ScheduleMode.SEQUENTIAL:
            return self._generate_sequential_sequence()
        else:
            return self._generate_random_sequence()

    def _generate_sequential_sequence(self) -> List[str]:
        """生成顺序模式的执行序列"""
        sequence = []
        
        if not self.config.sequence:
            self.logger.warning("顺序模式下未指定序列，使用默认配置")
            # 如果没有指定序列，使用fault_types生成
            for fault_type in self.config.fault_types:
                sequence.append(fault_type)
            if self.config.include_normal:
                sequence.append("normal")
            return sequence
        
        # 按指定顺序和数量生成序列
        for fault_type, count in self.config.sequence:
            for _ in range(count):
                sequence.append(fault_type)
        
        self.logger.info(f"顺序模式 - 生成序列: {len(sequence)} 次")
        self.logger.info(f"序列详情: {self.config.sequence}")
        
        return sequence

    def _generate_random_sequence(self) -> List[str]:
        """生成随机模式的执行序列"""
        sequence = []
        
        # 添加指定数量的每种故障
        for fault_type in self.config.fault_types:
            count = self.config.fault_counts.get(fault_type, 1)
            for _ in range(count):
                sequence.append(fault_type)
        
        # 添加normal任务
        if self.config.include_normal:
            normal_count = self.config.normal_count
            for _ in range(normal_count):
                sequence.append("normal")
        
        # 如果指定了总数，但当前数量不足，随机填充
        if self.config.total_count > 0 and len(sequence) < self.config.total_count:
            remaining = self.config.total_count - len(sequence)
            self.logger.info(f"随机填充 {remaining} 次任务")
            for _ in range(remaining):
                if self.config.fault_types:
                    sequence.append(random.choice(self.config.fault_types))
        
        # 随机打乱顺序
        random.shuffle(sequence)
        
        self.logger.info(f"随机模式 - 生成序列: {len(sequence)} 次")
        self.logger.info(f"故障分布: {self.config.fault_counts}")
        if self.config.include_normal:
            self.logger.info(f"Normal任务: {self.config.normal_count} 次")
        
        return sequence

    def should_stop(self) -> bool:
        """
        检查是否应该停止调度
        
        Returns:
            True if should stop
        """
        # 检查是否所有任务都已完成
        if not self.remaining_sequence:
            self.logger.info("所有任务已完成，停止调度")
            return True
        
        # 检查日志数量
        if self.config.max_logs and self.total_logs_collected >= self.config.max_logs:
            self.logger.info(f"达到最大日志数量 {self.config.max_logs}，停止调度")
            return True
        
        # 检查运行时长
        if self.config.max_duration:
            elapsed = time.time() - self.start_time
            if elapsed >= self.config.max_duration:
                self.logger.info(f"达到最大运行时长 {self.config.max_duration}s，停止调度")
                return True
        
        return False

    def get_next_interval(self) -> int:
        """
        获取下一次执行的间隔时间
        
        Returns:
            间隔秒数
        """
        return random.randint(self.config.interval_min, self.config.interval_max)

    def record_execution(self, fault_type: str, logs_collected: int = 0):
        """
        记录一次执行
        
        Args:
            fault_type: 执行的故障类型
            logs_collected: 本次收集的日志数量
        """
        self.execution_count += 1
        self.total_logs_collected += logs_collected
        self.fault_execution_count[fault_type] = self.fault_execution_count.get(fault_type, 0) + 1
        
        self.logger.debug(f"记录执行: {fault_type}, 日志: {logs_collected}, 总计: {self.execution_count}")

    def get_statistics(self) -> Dict:
        """
        获取调度统计信息
        
        Returns:
            统计字典
        """
        elapsed = time.time() - self.start_time
        return {
            "execution_count": self.execution_count,
            "total_logs_collected": self.total_logs_collected,
            "elapsed_time": elapsed,
            "fault_distribution": self.fault_execution_count.copy(),
            "mode": self.config.mode.value,
            "remaining_tasks": len(self.remaining_sequence)
        }

    def run(self, executor: Callable[[str], Dict]) -> List[Dict]:
        """
        运行调度器
        
        Args:
            executor: 执行函数，接收fault_type，返回执行结果字典
        
        Returns:
            所有执行结果列表
        """
        results = []
        
        # 生成执行序列
        sequence = self.generate_sequence()
        self.remaining_sequence = sequence.copy()
        
        total_tasks = len(sequence)
        
        self.logger.info("=" * 60)
        self.logger.info(f"开始调度 - 模式: {self.config.mode.value}")
        self.logger.info(f"总任务数: {total_tasks}")
        self.logger.info(f"间隔范围: {self.config.interval_min}s - {self.config.interval_max}s")
        self.logger.info("=" * 60)
        
        while self.remaining_sequence:
            if self.should_stop():
                break
            
            # 获取下一个任务
            fault_type = self.remaining_sequence.pop(0)
            
            current_index = total_tasks - len(self.remaining_sequence)
            self.logger.info(f"[{current_index}/{total_tasks}] 执行: {fault_type}")
            
            try:
                result = executor(fault_type)
                result["sequence_index"] = current_index
                result["fault_type"] = fault_type
                results.append(result)
                
                # 记录执行
                logs_collected = result.get("logs_collected", 0)
                self.record_execution(fault_type, logs_collected)
                
            except Exception as e:
                self.logger.error(f"执行 {fault_type} 失败: {e}")
                results.append({
                    "sequence_index": current_index,
                    "fault_type": fault_type,
                    "success": False,
                    "error": str(e)
                })
            
            # 等待间隔（如果还有剩余任务）
            if self.remaining_sequence and not self.should_stop():
                interval = self.get_next_interval()
                self.logger.info(f"等待 {interval} 秒后执行下一次...")
                time.sleep(interval)
        
        self.logger.info("=" * 60)
        self.logger.info(f"调度完成，共执行 {len(results)} 次")
        self.logger.info(f"故障分布: {self.fault_execution_count}")
        self.logger.info("=" * 60)
        
        return results


if __name__ == "__main__":
    # 测试代码 - 顺序模式
    logging.basicConfig(level=logging.INFO)
    
    print("\n" + "=" * 60)
    print("测试顺序模式")
    print("=" * 60)
    
    config_seq = SchedulerConfig(
        mode=ScheduleMode.SEQUENTIAL,
        sequence=[
            ("data_skew", 2),
            ("task_fail", 1),
            ("normal", 1),
            ("long_tail", 2)
        ],
        interval_min=1,
        interval_max=3
    )
    
    scheduler_seq = FaultScheduler(config_seq)
    
    def mock_executor(fault_type: str) -> Dict:
        print(f"  执行: {fault_type}")
        return {
            "success": True,
            "logs_collected": random.randint(100, 1000)
        }
    
    results_seq = scheduler_seq.run(mock_executor)
    print("\n顺序模式统计:", scheduler_seq.get_statistics())
    
    # 测试代码 - 随机模式
    print("\n" + "=" * 60)
    print("测试随机模式")
    print("=" * 60)
    
    config_rand = SchedulerConfig(
        mode=ScheduleMode.RANDOM,
        fault_types=["data_skew", "task_fail", "long_tail"],
        fault_counts={
            "data_skew": 3,
            "task_fail": 2,
            "long_tail": 2
        },
        include_normal=True,
        normal_count=2,
        interval_min=1,
        interval_max=3
    )
    
    scheduler_rand = FaultScheduler(config_rand)
    results_rand = scheduler_rand.run(mock_executor)
    print("\n随机模式统计:", scheduler_rand.get_statistics())
