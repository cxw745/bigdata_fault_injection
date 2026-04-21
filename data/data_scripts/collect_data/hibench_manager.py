#!/usr/bin/env python3
"""
HiBench数据生成管理模块

负责：
- 生成不同大小的测试数据
- 验证数据生成结果
- 与调度器集成

注意：HiBench安装在 /opt/HiBench
通过修改 /opt/HiBench/conf/hibench.conf 中的 hibench.scale.profile 来指定数据规模
"""
import os
import subprocess
import logging
import time
import re
from typing import Optional, Dict
from dataclasses import dataclass


@dataclass
class DataSizeConfig:
    """数据大小配置"""
    name: str  # 配置名称 (tiny, small, large, huge, gigantic)
    description: str  # 描述


# HiBench 数据规模配置
# 实际大小定义在 /opt/HiBench/conf/workloads/micro/wordcount.conf
HIBENCH_DATA_SIZES = {
    "tiny": DataSizeConfig(
        name="tiny",
        description="极小数据集 - 约32MB，用于快速验证"
    ),
    "small": DataSizeConfig(
        name="small",
        description="小数据集 - 约320MB，用于开发测试"
    ),
    "large": DataSizeConfig(
        name="large",
        description="大数据集 - 约32GB，用于性能测试"
    ),
    "huge": DataSizeConfig(
        name="huge",
        description="超大数据集 - 约320GB，用于压力测试"
    ),
    "gigantic": DataSizeConfig(
        name="gigantic",
        description="巨型数据集 - 约3.2TB，用于极限测试"
    ),
}


class HiBenchManager:
    """HiBench数据管理器"""

    def __init__(self,
                 hibench_home: str = "/opt/HiBench",
                 hadoop_home: str = "/opt/hadoop",
                 logger: Optional[logging.Logger] = None):
        self.hibench_home = hibench_home
        self.hadoop_home = hadoop_home
        self.logger = logger or logging.getLogger(__name__)

        # 配置路径
        self.hibench_conf = os.path.join(hibench_home, "conf/hibench.conf")
        self.prepare_script = os.path.join(hibench_home, "bin/workloads/micro/wordcount/prepare/prepare.sh")

        # 数据路径
        self.input_base = "/HiBench/HiBench/Wordcount/Input"

    def generate_data(self, size_config: DataSizeConfig, force: bool = False) -> bool:
        """
        生成指定大小的测试数据

        Args:
            size_config: 数据大小配置
            force: 是否强制重新生成

        Returns:
            True if success
        """
        self.logger.info(f"开始生成数据: {size_config.name} ({size_config.description})")

        # 检查是否已存在
        if not force and self._check_data_exists():
            current_profile = self._get_current_profile()
            if current_profile == size_config.name:
                self.logger.info(f"数据已存在且规模匹配({size_config.name})，跳过生成")
                return True
            else:
                self.logger.info(f"当前数据规模为 {current_profile}，需要重新生成 {size_config.name}")

        # 更新HiBench配置
        self._update_hibench_profile(size_config.name)

        # 执行数据生成脚本
        try:
            self.logger.info("执行数据生成脚本...")

            env = os.environ.copy()
            env["HIBENCH_HOME"] = self.hibench_home
            env["HADOOP_HOME"] = self.hadoop_home

            # 执行prepare脚本
            cmd = f"bash {self.prepare_script}"
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=600,
                env=env
            )

            if result.returncode != 0:
                self.logger.error(f"数据生成失败: {result.stderr}")
                return False

            # 验证数据
            if self._verify_data():
                self.logger.info(f"✓ 数据生成成功: {size_config.name}")
                return True
            else:
                self.logger.error(f"✗ 数据验证失败")
                return False

        except subprocess.TimeoutExpired:
            self.logger.error("数据生成超时")
            return False
        except Exception as e:
            self.logger.error(f"数据生成异常: {e}")
            return False

    def _update_hibench_profile(self, profile: str):
        """更新HiBench数据规模配置"""
        self.logger.info(f"更新HiBench配置: {self.hibench_conf}")
        self.logger.info(f"设置数据规模: {profile}")

        # 读取现有配置
        with open(self.hibench_conf, "r") as f:
            content = f.read()

        # 替换 profile 配置
        new_content = re.sub(
            r'hibench\.scale\.profile\s+\w+',
            f'hibench.scale.profile                {profile}',
            content
        )

        # 写回配置
        with open(self.hibench_conf, "w") as f:
            f.write(new_content)

        self.logger.debug(f"已更新配置: hibench.scale.profile = {profile}")

    def _get_current_profile(self) -> str:
        """获取当前配置的profile"""
        try:
            with open(self.hibench_conf, "r") as f:
                for line in f:
                    match = re.match(r'hibench\.scale\.profile\s+(\w+)', line)
                    if match:
                        return match.group(1)
        except Exception as e:
            self.logger.debug(f"获取当前profile失败: {e}")
        return "unknown"

    def _check_data_exists(self) -> bool:
        """检查数据是否已存在"""
        try:
            actual_size = self._get_data_size()
            if actual_size == 0:
                return False

            self.logger.info(f"数据已存在: {self._format_bytes(actual_size)}")
            return True

        except Exception as e:
            self.logger.debug(f"检查数据存在性失败: {e}")
            return False

    def _verify_data(self) -> bool:
        """验证生成的数据"""
        self.logger.info("验证数据...")

        try:
            # 等待HDFS同步
            time.sleep(2)

            # 检查输入目录是否存在
            cmd = f"{self.hadoop_home}/bin/hdfs dfs -ls {self.input_base} 2>&1"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)

            if "No such file" in result.stderr or result.returncode != 0:
                self.logger.error(f"输入目录不存在: {self.input_base}")
                return False

            # 获取实际数据大小
            actual_size = self._get_data_size()

            if actual_size > 0:
                self.logger.info(f"✓ 数据验证通过: {self._format_bytes(actual_size)}")
                return True
            else:
                self.logger.error(f"✗ 数据大小为0")
                return False

        except Exception as e:
            self.logger.error(f"数据验证异常: {e}")
            return False

    def _get_data_size(self) -> int:
        """获取当前数据大小（字节）"""
        try:
            cmd = f"{self.hadoop_home}/bin/hdfs dfs -du -s {self.input_base} 2>&1"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)

            if result.returncode != 0:
                return 0

            # 解析输出: "size  path"
            parts = result.stdout.strip().split()
            if len(parts) >= 1:
                return int(parts[0])
            return 0

        except Exception as e:
            self.logger.debug(f"获取数据大小失败: {e}")
            return 0

    def _format_bytes(self, bytes_val: int) -> str:
        """格式化字节数"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"

    def get_current_data_info(self) -> Dict:
        """获取当前数据信息"""
        size = self._get_data_size()
        profile = self._get_current_profile()
        return {
            "path": self.input_base,
            "size_bytes": size,
            "size_formatted": self._format_bytes(size),
            "profile": profile,
            "exists": size > 0
        }


def quick_generate_data(size_name: str = "tiny") -> bool:
    """
    快速生成数据的便捷函数

    Args:
        size_name: 数据大小名称 (tiny/small/large/huge/gigantic)

    Returns:
        True if success
    """
    logging.basicConfig(level=logging.INFO)

    if size_name not in HIBENCH_DATA_SIZES:
        logging.error(f"未知的数据大小: {size_name}")
        return False

    manager = HiBenchManager()
    config = HIBENCH_DATA_SIZES[size_name]

    return manager.generate_data(config)


if __name__ == "__main__":
    # 测试
    logging.basicConfig(level=logging.INFO)

    manager = HiBenchManager()

    # 生成tiny大小数据
    success = manager.generate_data(HIBENCH_DATA_SIZES["tiny"])
    print(f"生成结果: {'成功' if success else '失败'}")

    # 打印当前数据信息
    info = manager.get_current_data_info()
    print(f"当前数据: {info}")
