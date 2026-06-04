# MapReduce任务日志标记使用指南

## 概述

故障标记模块V4支持将标记写入MapReduce任务的容器日志（syslog），这样可以在分析任务日志时直接看到故障注入的标记，便于验证故障是否成功注入。

## 支持的故障类型

以下故障类型支持写入MR日志（在Mapper/Reducer中注入）：

| 故障类型 | 说明 | 注入位置 |
|---------|------|---------|
| `long_tail` | 长尾任务故障注入 | Mapper |
| `task_fail` | 任务失败故障注入 | Mapper |
| `data_skew` | 数据倾斜故障注入 | Mapper |
| `data_bloat` | 数据膨胀故障注入 | Mapper |

## 使用方法

### 1. 在Mapper/Reducer中导入模块

```python
#!/usr/bin/env python3
import os
import sys

# 添加collect_data到路径
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker_v4 import mark_fault_start, mark_fault_end, mark_fault_injection

# 启用MR日志写入
os.environ["FAULT_MARKER_ENABLED"] = "true"
os.environ["FAULT_MARKER_MR_LOGS"] = "true"
```

### 2. 在故障注入点写入标记

```python
def main():
    # 标记故障注入开始
    mark_fault_start("long_tail", {
        "task_id": task_id,
        "container_id": container_id,
        "duration": 120
    })
    
    # 标记注入动作
    mark_fault_injection("long_tail", "sleep_delay", f"Mapper-{task_id}", 120)
    
    # 执行故障注入（如sleep 120秒）
    time.sleep(120)
    
    # 标记故障注入结束
    mark_fault_end("long_tail", {
        "result": "success",
        "actual_duration": 120
    })
```

### 3. 完整示例

参考文件：`long_tail/mapper_long_tail_with_marker.py`

这个示例展示了如何在Mapper中：
- 判断当前任务是否是目标注入任务
- 在故障注入开始和结束时写入标记
- 记录注入动作详情

## 查看MR日志中的标记

### 方法1: 查看特定容器的日志

```bash
# 找到任务的容器日志目录
ls /opt/hadoop/logs/userlogs/application_*/container_*/

# 查看特定容器的syslog
cat /opt/hadoop/logs/userlogs/application_xxx/container_xxx/syslog | grep FAULT_INJECTION
```

### 方法2: 查看所有MR容器的故障标记

```bash
# 查找所有MR容器日志中的故障标记
grep "FAULT_INJECTION" /opt/hadoop/logs/userlogs/application_*/container_*/syslog 2>/dev/null

# 查找特定故障类型的标记
grep "LONG_TAIL_INJECTION_START" /opt/hadoop/logs/userlogs/application_*/container_*/syslog 2>/dev/null

# 查看最近的标记（按时间排序）
grep "FAULT_INJECTION" /opt/hadoop/logs/userlogs/application_*/container_*/syslog 2>/dev/null | tail -20
```

### 方法3: 通过YARN命令查看

```bash
# 查看应用的容器列表
yarn application -list-containers application_xxx

# 查看特定容器的日志
yarn logs -applicationId application_xxx -containerId container_xxx | grep FAULT_INJECTION
```

## 日志格式

MR任务日志中的标记格式：

```
2026-03-18 13:15:32,123 INFO [FAULT_INJECTION] {"event": "LONG_TAIL_INJECTION_START", "fault_type": "long_tail", ...}
```

包含的信息：
- 时间戳（与Hadoop日志格式一致）
- 日志级别（INFO）
- 标记标识（[FAULT_INJECTION]）
- JSON格式的标记数据

## 开关控制

### 环境变量控制

```bash
# 启用标记功能
export FAULT_MARKER_ENABLED=true

# 启用MR日志写入
export FAULT_MARKER_MR_LOGS=true

# 启用Hadoop组件日志写入（可选）
export FAULT_MARKER_HADOOP_LOGS=true

# 设置本地日志路径（可选）
export FAULT_MARKER_LOG_PATH=/tmp/fault_markers.log
```

### 代码控制

```python
from collect_data.fault_marker_v4 import FaultMarker

marker = FaultMarker({
    "enabled": True,
    "write_to_mr_logs": True,        # 控制MR日志
    "write_to_hadoop_logs": False,   # 控制Hadoop日志
    "print_to_console": True         # 是否在控制台打印
})
```

## 验证故障注入

### 步骤1: 运行MapReduce任务

使用带标记的Mapper运行任务：

```bash
python3 long_tail/inject_long_tail.py
```

### 步骤2: 查看任务日志

```bash
# 找到最新的应用ID
ls -lt /opt/hadoop/logs/userlogs/ | head -5

# 查看该应用的所有故障标记
APP_ID="application_xxx"
grep "FAULT_INJECTION" /opt/hadoop/logs/userlogs/${APP_ID}/container_*/syslog 2>/dev/null
```

### 步骤3: 验证标记内容

标记应该包含：
- `LONG_TAIL_INJECTION_START` - 故障注入开始
- `FAULT_INJECTION_ACTION` - 注入动作详情
- `LONG_TAIL_INJECTION_END` - 故障注入结束

## 故障排查

### 问题1: 找不到MR日志文件

**原因**: 当前不在MR容器中运行

**解决**: 确保代码在Mapper/Reducer中执行，而不是在客户端直接运行

### 问题2: 标记未写入MR日志

**检查项**:
1. 环境变量 `FAULT_MARKER_MR_LOGS` 是否设置为 `true`
2. 故障类型是否支持MR日志（检查 `mr_log_target` 配置）
3. 是否有写入权限

### 问题3: 找不到历史标记

**检查**:
```bash
# 确认日志目录存在
ls /opt/hadoop/logs/userlogs/

# 确认容器目录存在
ls /opt/hadoop/logs/userlogs/application_*/

# 确认syslog文件存在
ls /opt/hadoop/logs/userlogs/application_*/container_*/syslog
```

## 注意事项

1. **MR日志只在容器运行时存在** - 任务完成后，日志会被聚合到HDFS，本地日志可能被清理
2. **日志写入是追加模式** - 多次运行任务会在同一日志文件中追加标记
3. **性能影响** - 日志写入对性能影响很小，但在高并发场景下可以考虑关闭控制台打印

## 示例输出

```
2026-03-18 13:15:32,123 INFO [FAULT_INJECTION] {"event": "LONG_TAIL_INJECTION_START", "fault_type": "long_tail", "description": "长尾任务故障注入", "phase": "START", "timestamp": "2026-03-18T13:15:32.123456", "unix_time": 1773844532.123456, "details": {"task_id": 0, "container_id": "container_xxx", "duration": 120}}
2026-03-18 13:15:32,456 INFO [FAULT_INJECTION] {"event": "FAULT_INJECTION_ACTION", "fault_type": "long_tail", "description": "长尾任务故障注入", "action": "sleep_delay", "timestamp": "2026-03-18T13:15:32.456789", "unix_time": 1773844532.456789, "target": "Mapper-0", "duration": 120}
2026-03-18 13:17:32,789 INFO [FAULT_INJECTION] {"event": "LONG_TAIL_INJECTION_END", "fault_type": "long_tail", "description": "长尾任务故障注入", "phase": "END", "timestamp": "2026-03-18T13:17:32.789012", "unix_time": 1773844652.789012, "details": {"result": "success", "actual_duration": 120}}
```
