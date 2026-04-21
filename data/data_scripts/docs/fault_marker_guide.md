# 故障标记功能使用指南

## 概述

故障标记功能用于在故障注入时添加标记日志，方便验证故障是否成功注入。支持统一配置和不同故障类型的自定义标记。

## 核心特性

- ✅ 统一配置：在一个文件中设置所有标记参数
- ✅ 类型定制：不同故障类型可配置不同的标记内容
- ✅ 灵活启用：支持全局启用/禁用或按类型启用/禁用
- ✅ 多种配置方式：代码配置、环境变量配置
- ✅ 统计查询：支持查询和统计标记记录

---

## 快速开始

### 1. 基本使用

```python
from collect_data.fault_marker import FaultMarker

# 创建标记器并启用
marker = FaultMarker({"enabled": True})

# 标记故障开始
marker.mark_start("data_skew")

# 标记故障结束
marker.mark_end("data_skew")
```

### 2. 通过环境变量启用

```bash
export FAULT_MARKER_ENABLED=true
export FAULT_MARKER_LOG_PATH=/tmp/fault_markers.log
```

然后在代码中直接使用：

```python
from collect_data.fault_marker import FaultMarker

marker = FaultMarker()  # 自动读取环境变量
marker.mark_start("task_fail")
marker.mark_end("task_fail")
```

---

## 配置方法

### 方法1：代码中配置（推荐）

```python
from collect_data.fault_marker import FaultMarker

marker = FaultMarker({
    "enabled": True,                           # 启用标记
    "log_path": "/tmp/fault_markers.log",      # 日志文件路径
    "default_start_marker": "FAULT_START",     # 默认开始标记
    "default_end_marker": "FAULT_END",         # 默认结束标记
    "include_timestamp": True,                 # 包含时间戳
    "include_unix_time": True,                 # 包含Unix时间
    "print_to_console": True                   # 打印到控制台
})
```

### 方法2：环境变量配置

| 环境变量 | 说明 | 示例 |
|---------|------|------|
| `FAULT_MARKER_ENABLED` | 是否启用标记 | `true` / `false` |
| `FAULT_MARKER_LOG_PATH` | 日志文件路径 | `/tmp/fault_markers.log` |

```bash
export FAULT_MARKER_ENABLED=true
export FAULT_MARKER_LOG_PATH=/tmp/my_markers.log
```

### 方法3：获取全局单例

```python
from collect_data.fault_marker import get_marker

# 首次调用时传入配置
marker = get_marker({"enabled": True, "log_path": "/tmp/markers.log"})

# 后续调用返回同一个实例
marker2 = get_marker()  # 返回相同的实例
```

---

## 标记操作

### 标记故障开始

```python
# 基本用法
marker.mark_start("data_skew")

# 带详细信息
marker.mark_start("data_skew", {
    "skew_ratio": 0.8,
    "target_key": "hot_key"
})
```

### 标记故障注入动作

```python
# 基本用法
marker.mark_injection("wait_time", "SIGSTOP")

# 带目标和持续时间
marker.mark_injection(
    fault_type="wait_time",
    action="SIGSTOP",
    target="ResourceManager",
    duration=120
)
```

### 标记故障结束

```python
# 基本用法
marker.mark_end("data_skew")

# 带结果信息
marker.mark_end("data_skew", {
    "result": "success",
    "affected_tasks": 5
})
```

---

## 自定义故障类型标记

### 为现有故障类型修改配置

```python
# 修改 data_skew 的标记配置
marker.set_fault_config(
    "data_skew",
    enabled=True,
    start_marker="DATA_SKEW_BEGIN",
    end_marker="DATA_SKEW_FINISH",
    description="数据倾斜故障"
)
```

### 添加新的故障类型

```python
# 定义新的故障类型
marker.set_fault_config(
    "my_custom_fault",
    enabled=True,
    start_marker="MY_FAULT_START",
    end_marker="MY_FAULT_END",
    description="我的自定义故障"
)

# 使用新的故障类型
marker.mark_start("my_custom_fault")
marker.mark_end("my_custom_fault")
```

---

## 预定义故障类型

| 故障类型 | 开始标记 | 结束标记 | 描述 | 默认启用 |
|---------|---------|---------|------|---------|
| `data_skew` | DATA_SKEW_INJECTION_START | DATA_SKEW_INJECTION_END | 数据倾斜故障注入 | ✅ |
| `data_bloat` | DATA_BLOAT_INJECTION_START | DATA_BLOAT_INJECTION_END | 数据膨胀故障注入 | ✅ |
| `task_fail` | TASK_FAIL_INJECTION_START | TASK_FAIL_INJECTION_END | 任务失败故障注入 | ✅ |
| `long_tail` | LONG_TAIL_INJECTION_START | LONG_TAIL_INJECTION_END | 长尾任务故障注入 | ✅ |
| `wait_time` | WAIT_TIME_INJECTION_START | WAIT_TIME_INJECTION_END | 等待时间异常注入 | ✅ |
| `runtime_delta` | RUNTIME_DELTA_INJECTION_START | RUNTIME_DELTA_INJECTION_END | 运行时间异常注入 | ✅ |
| `exit_time` | EXIT_TIME_INJECTION_START | EXIT_TIME_INJECTION_END | 退出时间异常注入 | ✅ |
| `network_latency` | NETWORK_LATENCY_INJECTION_START | NETWORK_LATENCY_INJECTION_END | 网络延迟故障注入 | ✅ |
| `normal` | NORMAL_TASK_START | NORMAL_TASK_END | 正常任务执行 | ❌ |

---

## 查询和统计

### 获取所有标记

```python
# 获取所有标记
all_markers = marker.get_markers()

# 获取特定故障类型的标记
data_skew_markers = marker.get_markers(fault_type="data_skew")

# 获取特定阶段的标记
start_markers = marker.get_markers(phase="START")

# 组合条件查询
markers = marker.get_markers(
    fault_type="data_skew",
    phase="START",
    start_time=1773826000,
    end_time=1773827000
)
```

### 获取统计信息

```python
# 获取所有标记统计
stats = marker.get_statistics()
print(f"总标记数: {stats['total_markers']}")
print(f"开始标记: {stats['start_markers']}")
print(f"结束标记: {stats['end_markers']}")
print(f"动作标记: {stats['action_markers']}")
print(f"按故障类型: {stats['by_fault_type']}")

# 获取特定故障类型的统计
data_skew_stats = marker.get_statistics(fault_type="data_skew")
```

### 清除标记

```python
# 清除所有标记
marker.clear_markers()
```

---

## 标记文件格式

标记文件为JSON Lines格式，每行一个JSON对象：

```json
{"event": "DATA_SKEW_INJECTION_START", "fault_type": "data_skew", "description": "数据倾斜故障注入", "phase": "START", "timestamp": "2026-03-18T09:30:03.469199", "unix_time": 1773826203.4696348, "details": {"skew_ratio": 0.8}}
{"event": "FAULT_INJECTION_ACTION", "fault_type": "data_skew", "description": "数据倾斜故障注入", "action": "inject_skew_key", "timestamp": "2026-03-18T09:30:03.469967", "unix_time": 1773826203.4699752, "target": "Mapper", "duration": 120}
{"event": "DATA_SKEW_INJECTION_END", "fault_type": "data_skew", "description": "数据倾斜故障注入", "phase": "END", "timestamp": "2026-03-18T09:30:03.470120", "unix_time": 1773826203.4701252, "details": {"result": "success"}}
```

### 字段说明

| 字段 | 说明 | 示例 |
|------|------|------|
| `event` | 事件类型 | `DATA_SKEW_INJECTION_START` |
| `fault_type` | 故障类型 | `data_skew` |
| `description` | 故障描述 | `数据倾斜故障注入` |
| `phase` | 阶段 | `START` / `END` |
| `timestamp` | ISO格式时间戳 | `2026-03-18T09:30:03.469199` |
| `unix_time` | Unix时间戳 | `1773826203.4696348` |
| `details` | 额外详情 | `{"skew_ratio": 0.8}` |
| `action` | 注入动作 | `inject_skew_key` |
| `target` | 目标 | `Mapper` |
| `duration` | 持续时间 | `120` |

---

## 完整示例

### 示例1：在故障注入脚本中使用

```python
#!/usr/bin/env python3
from collect_data.fault_marker import FaultMarker

# 初始化标记器
marker = FaultMarker({"enabled": True})

def inject_data_skew():
    # 标记开始
    marker.mark_start("data_skew", {"skew_ratio": 0.8})
    
    try:
        # 执行故障注入
        print("注入数据倾斜故障...")
        marker.mark_injection("data_skew", "inject_skew_key", "Mapper", 120)
        
        # 模拟故障执行
        import time
        time.sleep(2)
        
        # 标记结束
        marker.mark_end("data_skew", {"result": "success"})
        
    except Exception as e:
        # 标记失败
        marker.mark_end("data_skew", {"result": "failed", "error": str(e)})
        raise

if __name__ == "__main__":
    inject_data_skew()
    
    # 查看统计
    stats = marker.get_statistics()
    print(f"\n故障注入完成，共产生 {stats['total_markers']} 个标记")
```

### 示例2：批量故障注入

```python
from collect_data.fault_marker import FaultMarker

marker = FaultMarker({"enabled": True})

faults = [
    ("data_skew", {"skew_ratio": 0.8}),
    ("task_fail", {"fail_rate": 0.2}),
    ("long_tail", {"sleep_time": 120})
]

for fault_type, details in faults:
    marker.mark_start(fault_type, details)
    print(f"执行 {fault_type} 故障注入...")
    marker.mark_injection(fault_type, "execute", "target", 120)
    marker.mark_end(fault_type, {"status": "completed"})

# 打印统计
stats = marker.get_statistics()
print(f"\n批量注入完成:")
for fault_type, counts in stats['by_fault_type'].items():
    print(f"  {fault_type}: 开始={counts['start']}, 结束={counts['end']}")
```

### 示例3：与调度器集成

```python
from collect_data.unified_scheduler_v2 import UnifiedScheduler
from collect_data.scheduler_core import SchedulerConfig, ScheduleMode
from collect_data.fault_marker import FaultMarker

# 创建标记器
marker = FaultMarker({"enabled": True})

# 定义带标记的执行函数
def execute_with_marker(fault_type):
    # 标记开始
    marker.mark_start(fault_type)
    
    # 执行实际的故障注入（这里简化处理）
    print(f"执行故障注入: {fault_type}")
    
    # 标记结束
    marker.mark_end(fault_type)
    
    return {"success": True, "logs_collected": 100}

# 配置调度器
config = SchedulerConfig(
    mode=ScheduleMode.SEQUENTIAL,
    sequence=[("data_skew", 2), ("task_fail", 1)],
    interval_min=10,
    interval_max=20
)

# 运行调度器
scheduler = FaultScheduler(config)
results = scheduler.run(execute_with_marker)

# 查看标记统计
stats = marker.get_statistics()
print(f"\n标记统计: {stats}")
```

---

## 兼容旧版API

如果之前使用过旧版标记功能，可以继续使用以下函数：

```python
from collect_data.fault_marker import (
    mark_fault_start,
    mark_fault_end,
    mark_fault_injection,
    is_marker_enabled
)

# 需要先设置环境变量启用标记
import os
os.environ["FAULT_MARKER_ENABLED"] = "true"

# 使用旧版API
mark_fault_start("data_skew")
mark_fault_injection("data_skew", "Mapper", "inject_skew_key", 120)
mark_fault_end("data_skew")

# 检查是否启用
if is_marker_enabled():
    print("标记功能已启用")
```

---

## 注意事项

1. **默认禁用**：标记功能默认禁用，需要显式启用
2. **文件权限**：确保有写入日志文件的权限
3. **性能影响**：频繁写入可能影响性能，生产环境建议适当控制标记频率
4. **日志清理**：定期清理标记日志文件，避免占用过多磁盘空间
