# 故障标记功能验证报告

## 测试概述

本报告记录了所有故障类型的标记功能测试结果，验证故障标记系统是否正常工作。

**测试时间**: 2026-03-18  
**测试脚本**: `/project/data/data_scripts/test_all_fault_markers.py`

---

## 测试结果汇总

| 测试项目 | 状态 | 说明 |
|---------|------|------|
| 基本标记功能 | ✅ 通过 | 所有故障类型标记正确生成 |
| 单独故障类型测试 | ✅ 通过 | 6种主要故障类型均通过验证 |
| 标记格式验证 | ✅ 通过 | JSON格式正确，字段完整 |
| 启用/禁用功能 | ✅ 通过 | 控制逻辑正常 |
| 查询统计功能 | ✅ 通过 | 数据查询和统计准确 |

**总体结果**: ✅ **所有测试通过**

---

## 故障类型标记配置

### 已实现的故障类型标记

| 故障类型 | 开始标记 | 结束标记 | 描述 | 状态 |
|---------|---------|---------|------|------|
| `data_skew` | DATA_SKEW_INJECTION_START | DATA_SKEW_INJECTION_END | 数据倾斜故障注入 | ✅ 已配置 |
| `data_bloat` | DATA_BLOAT_INJECTION_START | DATA_BLOAT_INJECTION_END | 数据膨胀故障注入 | ✅ 已配置 |
| `task_fail` | TASK_FAIL_INJECTION_START | TASK_FAIL_INJECTION_END | 任务失败故障注入 | ✅ 已配置 |
| `long_tail` | LONG_TAIL_INJECTION_START | LONG_TAIL_INJECTION_END | 长尾任务故障注入 | ✅ 已配置 |
| `wait_time` | WAIT_TIME_INJECTION_START | WAIT_TIME_INJECTION_END | 等待时间异常注入 | ✅ 已配置 |
| `runtime_delta` | RUNTIME_DELTA_INJECTION_START | RUNTIME_DELTA_INJECTION_END | 运行时间异常注入 | ✅ 已配置 |
| `exit_time` | EXIT_TIME_INJECTION_START | EXIT_TIME_INJECTION_END | 退出时间异常注入 | ✅ 已配置 |
| `network_latency` | NETWORK_LATENCY_INJECTION_START | NETWORK_LATENCY_INJECTION_END | 网络延迟故障注入 | ✅ 已配置 |
| `normal` | NORMAL_TASK_START | NORMAL_TASK_END | 正常任务执行 | ⚠️ 默认禁用 |

### 脚本集成状态

| 故障脚本 | 标记集成 | 开始标记 | 注入标记 | 结束标记 |
|---------|---------|---------|---------|---------|
| `long_tail/inject_long_tail.py` | ✅ 已集成 | ✅ | ✅ | ✅ |
| `task_fail/inject_task_fail.py` | ✅ 已集成 | ✅ | - | ✅ |
| `wait_time/inject_wait_time.py` | ✅ 已集成 | ✅ | ✅ | ✅ |
| `run_time/inject_runtime_delta.py` | ✅ 已集成 | ✅ | ✅ | ✅ |
| `exit_time/inject_exit_time.py` | ✅ 已集成 | ✅ | ✅ | ✅ |
| `network_latency/inject_network_latency.py` | ✅ 已集成 | ✅ | ✅ | ✅ |

---

## 标记格式示例

### 开始标记
```json
{
  "event": "DATA_SKEW_INJECTION_START",
  "fault_type": "data_skew",
  "description": "数据倾斜故障注入",
  "phase": "START",
  "timestamp": "2026-03-18T12:43:18.432898",
  "unix_time": 1773837798.4329042,
  "details": {
    "skew_ratio": 0.8,
    "target_key": "hot_key"
  }
}
```

### 注入动作标记
```json
{
  "event": "FAULT_INJECTION_ACTION",
  "fault_type": "wait_time",
  "description": "等待时间异常注入",
  "action": "SIGSTOP",
  "timestamp": "2026-03-18T12:43:18.469967",
  "unix_time": 1773837798.4699752,
  "target": "ResourceManager",
  "duration": 120
}
```

### 结束标记
```json
{
  "event": "DATA_SKEW_INJECTION_END",
  "fault_type": "data_skew",
  "description": "数据倾斜故障注入",
  "phase": "END",
  "timestamp": "2026-03-18T12:43:20.470120",
  "unix_time": 1773837820.4701252,
  "details": {
    "result": "success"
  }
}
```

---

## 使用方法

### 1. 在故障注入脚本中使用

```python
import sys
import os

# 添加collect_data到路径
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection

# 标记故障开始
mark_fault_start("long_tail", {"mode": "task_id", "duration": 120})

# 执行故障注入...
mark_fault_injection("long_tail", "sleep_delay", "Mapper", 120)

# 标记故障结束
mark_fault_end("long_tail", {"result": "success"})
```

### 2. 通过环境变量启用标记

```bash
export FAULT_MARKER_ENABLED=true
export FAULT_MARKER_LOG_PATH=/tmp/fault_markers.log
```

### 3. 查看标记日志

```bash
# 查看实时标记
tail -f /tmp/fault_markers.log

# 查询特定故障类型的标记
grep "long_tail" /tmp/fault_markers.log

# 统计标记数量
wc -l /tmp/fault_markers.log
```

---

## 验证故障注入的方法

### 方法1: 查看标记日志
```bash
# 查看所有标记
cat /tmp/fault_markers.log | python3 -m json.tool

# 查找特定故障的开始标记
grep "LONG_TAIL_INJECTION_START" /tmp/fault_markers.log

# 查找特定故障的结束标记
grep "LONG_TAIL_INJECTION_END" /tmp/fault_markers.log
```

### 方法2: 使用Python脚本查询
```python
from collect_data.fault_marker import FaultMarker

marker = FaultMarker({"enabled": True, "log_path": "/tmp/fault_markers.log"})

# 获取所有标记
all_markers = marker.get_markers()

# 获取特定故障类型的标记
long_tail_markers = marker.get_markers(fault_type="long_tail")

# 获取统计信息
stats = marker.get_statistics()
print(f"总标记数: {stats['total_markers']}")
print(f"按故障类型: {stats['by_fault_type']}")
```

### 方法3: 时间戳验证
通过比较标记时间戳和系统日志时间戳，可以验证故障注入的时序：
```bash
# 查看标记时间戳
grep "INJECTION_START" /tmp/fault_markers.log | jq '.timestamp'

# 对比YARN日志时间
grep "RUNNING" /opt/hadoop/logs/yarn-*.log
```

---

## 故障排查

### 问题1: 标记未写入
**可能原因**:
- 标记功能未启用（`FAULT_MARKER_ENABLED` 未设置或设置为 `false`）
- 日志文件路径无写入权限
- 脚本未正确导入标记模块

**解决方法**:
```bash
# 检查环境变量
echo $FAULT_MARKER_ENABLED  # 应输出 true

# 检查文件权限
touch /tmp/fault_markers.log
ls -la /tmp/fault_markers.log
```

### 问题2: 标记格式错误
**可能原因**:
- 日志文件被其他程序修改
- 磁盘空间不足

**解决方法**:
```bash
# 清理并重新创建日志文件
rm /tmp/fault_markers.log
touch /tmp/fault_markers.log
```

### 问题3: 查询不到标记
**可能原因**:
- 查询的故障类型名称拼写错误
- 日志文件路径配置不一致

**解决方法**:
```python
# 确认日志路径
import os
print(os.environ.get("FAULT_MARKER_LOG_PATH", "/tmp/fault_markers.log"))

# 列出所有故障类型
from collect_data.fault_marker import FAULT_MARKER_CONFIG
print(list(FAULT_MARKER_CONFIG.keys()))
```

---

## 结论

✅ **所有故障类型的标记功能已正确实现并验证通过**

- 8种故障类型（data_skew, data_bloat, task_fail, long_tail, wait_time, runtime_delta, exit_time, network_latency）均已配置标记
- 6个主要故障注入脚本均已集成标记功能
- 标记格式统一，包含完整的时间戳和详细信息
- 支持启用/禁用控制和查询统计功能

在故障注入实验中使用标记功能，可以：
1. **验证故障确实被注入** - 通过查看标记日志确认
2. **精确记录故障时间** - 用于后续分析和算法验证
3. **区分不同故障类型** - 每种故障有独特的标记标识
4. **统计故障注入情况** - 便于实验数据管理和分析
