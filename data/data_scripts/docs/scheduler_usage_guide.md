# 故障注入调度器使用指南 V3

## 目录结构

```
/project/data/data_scripts/
├── batch_scheduler.sh               # 批量调度管理脚本（推荐使用）
├── collect_data/                    # 核心模块
│   ├── unified_scheduler_v2.py     # 主调度器
│   ├── unified_config.py           # 统一配置
│   ├── scheduler_core.py           # 调度核心逻辑
│   ├── log_collector.py            # 日志收集
│   ├── metrics_collector.py        # 指标收集
│   ├── run_recorder.py             # 运行记录
│   ├── hibench_manager.py          # HiBench数据管理
│   └── fault_marker.py             # 故障标记
├── collect_data/data/              # 数据输出目录
│   └── batch_YYYYMMDD_HHMMSS/      # 批次目录
│       ├── execution_records.csv
│       ├── fault_labels.csv
│       ├── batch_summary.json
│       └── <fault_type>_<timestamp>/
├── logs/                           # 调度器日志
├── common_mapreduce/               # 基准任务
├── data_skew/                      # 数据倾斜故障
├── data_bloat/                     # 数据膨胀故障
├── task_fail/                      # 任务失败故障
├── long_tail/                      # 长尾任务故障
├── wait_time/                      # 等待时间异常
├── run_time/                       # 运行时间异常
├── exit_time/                      # 退出时间异常
└── network_latency/                # 网络延迟故障
```

---

## 快速开始

### 1. 使用批量调度脚本（推荐）

```bash
cd /project/data/data_scripts

# 启动批量调度（300次任务，自动分批）
TOTAL_RUNS=300 NORMAL_RATIO=0.3 FAULT_TYPES="wait_time,exit_time" ./batch_scheduler.sh start

# 查看状态
./batch_scheduler.sh status

# 停止调度
./batch_scheduler.sh stop
```

### 2. 直接使用Python调度器

```bash
cd /project/data/data_scripts/collect_data

# 顺序模式
python3 unified_scheduler_v2.py \
    --mode sequential \
    --sequence "wait_time:2,exit_time:1,wordcount:1" \
    --data-size small \
    --max-batch-size 50

# 随机模式
python3 unified_scheduler_v2.py \
    --mode random \
    --fault-types wait_time exit_time \
    --fault-counts "wait_time:3,exit_time:2" \
    --normal-count 2 \
    --data-size small
```

---

## 环境变量配置

批量调度脚本支持以下环境变量：

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `TOTAL_RUNS` | 总任务数 | 300 |
| `NORMAL_RATIO` | 正常任务比例 | 0.3 |
| `FAULT_TYPES` | 故障类型（逗号分隔） | wait_time,exit_time |
| `INTERVAL_MIN` | 最小间隔秒数 | 60 |
| `INTERVAL_MAX` | 最大间隔秒数 | 120 |
| `DATA_SIZE` | 数据大小 | small |
| `MAX_BATCH_SIZE` | 每批次最大任务数 | 50 |

### 示例命令

```bash
# 100次任务，20%正常，80%故障
TOTAL_RUNS=100 NORMAL_RATIO=0.2 ./batch_scheduler.sh start

# 自定义故障类型
FAULT_TYPES="wait_time,exit_time,runtime_delta" ./batch_scheduler.sh start

# 快速测试（小间隔）
INTERVAL_MIN=5 INTERVAL_MAX=10 TOTAL_RUNS=10 ./batch_scheduler.sh start

# 每批次30次任务
MAX_BATCH_SIZE=30 TOTAL_RUNS=100 ./batch_scheduler.sh start
```

---

## 状态显示

运行 `./batch_scheduler.sh status` 会显示：

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                         批量调度器运行状态                                     ║
╚═══════════════════════════════════════════════════════════════════════════════╝

┌─────────────────────────────────────────────────────────────────────────────┐
│                              进程状态                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│ 状态              : ✓ 运行中                                        │
│ PID                 : 2330491                                              │
│ 运行时间        : 09:16                                                │
│ CPU使用率        : 0.4%                                                 │
│ 内存使用率     : 0.4%                                                 │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                           当前批次执行进度                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ 当前批次        : batch_20260319_182810                                │
│ 已完成任务     : 3 次                                                │
│ 成功任务        : 3 次                                                │
├─────────────────────────────────────────────────────────────────────────────┤
│ 故障类型分布:                                                                │
│   wordcount              1 次                                              │
│   wait_time              1 次                                              │
│   exit_time              1 次                                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                           历史批次执行统计                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ batch_20260319_182810    │   3次 │ 成功:  3 │ wordcount wait_time... │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 命令行参数详解

### 通用参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--output-dir` | 输出目录 | `collect_data/data/` |
| `--data-size` | 数据大小 | `tiny` |
| `--max-logs` | 最大日志数 | 无限制 |
| `--max-duration` | 最大运行时长(秒) | 无限制 |
| `--interval-min` | 最小间隔(秒) | 60 |
| `--interval-max` | 最大间隔(秒) | 300 |
| `--max-batch-size` | 每批次最大任务数 | 50 |

### 顺序模式参数

| 参数 | 说明 | 示例 |
|------|------|------|
| `--mode sequential` | 使用顺序模式 | - |
| `--sequence` | 执行序列 | `"data_skew:2,task_fail:1,normal:1"` |

### 随机模式参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--mode random` | 使用随机模式 | - |
| `--fault-types` | 故障类型列表 | `data_skew task_fail long_tail` |
| `--fault-counts` | 每种故障数量 | `"data_skew:3,task_fail:2"` |
| `--total-count` | 总执行次数 | 根据fault-counts计算 |
| `--normal-count` | normal任务数量 | 0 |
| `--include-normal` | 包含无故障执行 | True |

---

## 数据大小选项

| 大小 | 描述 | 数据量 |
|------|------|--------|
| `tiny` | 快速验证 | ~32MB |
| `small` | 开发测试 | ~320MB |
| `large` | 性能测试 | ~32GB |
| `huge` | 压力测试 | ~320GB |
| `gigantic` | 极限测试 | ~3.2TB |

---

## 输出数据格式

### 批次目录结构

```
collect_data/data/
└── batch_20260319_182810/
    ├── execution_records.csv        # 执行记录
    ├── fault_labels.csv             # 故障标签
    ├── batch_summary.json           # 批次摘要
    ├── wait_time_20260319_182810/   # 任务目录
    │   ├── logs/
    │   │   ├── cpf-1/
    │   │   │   ├── namenode_*.csv
    │   │   │   └── resourcemanager_*.csv
    │   │   └── ...
    │   └── metrics/
    │       └── ...
    └── exit_time_20260319_183128/
```

### execution_records.csv

| 字段 | 说明 |
|------|------|
| seq_idx | 序列索引 |
| fault_type | 故障类型 |
| start_time | 开始时间 |
| end_time | 结束时间 |
| duration | 执行时长（秒） |
| success | 是否成功 |
| job_id | Job ID |
| application_id | Application ID |
| logs_dir | 日志目录 |
| metrics_dir | 指标目录 |
| error | 错误信息 |
| nodes | 受影响节点 |
| method | 注入方法 |
| task_dir | 任务目录 |

### fault_labels.csv

| 字段 | 说明 |
|------|------|
| folder_name | 文件夹名 |
| fault_type | 故障类型 |
| start_time | 开始时间 |
| end_time | 结束时间 |
| label | 故障标签（0-8） |

### batch_summary.json

```json
{
  "batch_number": 1,
  "total_batches": 6,
  "batch_dir": "/path/to/batch_20260319_182810",
  "completed_at": "2026-03-19T18:35:00"
}
```

---

## 自定义配置

### 修改 `unified_config.py`

#### 1. 修改输出目录

```python
OUTPUT_BASE = "/your/custom/path"
```

#### 2. 修改集群拓扑

```python
TOPOLOGY = {
    "master": "cpf-1",
    "slaves": ["cpf-2", "cpf-3", "cpf-4"],
    "all_nodes": ["cpf-1", "cpf-2", "cpf-3", "cpf-4"],
    "roles": {
        "cpf-1": ["namenode", "resourcemanager", "historyserver"],
        "cpf-2": ["datanode", "nodemanager"],
        ...
    }
}
```

#### 3. 添加新故障类型

```python
FAULT_CONFIG = {
    ...
    "new_fault_type": {
        "description": "新故障类型描述",
        "affected_nodes": ["cpf-2", "cpf-3"],
        "affected_services": ["nodemanager"],
        "injection_method": "故障注入方法描述",
        "nodes_known": True,
        "inject_stage": "dir-run",
        "script_type": "py",
        "script_dir": "new_fault",
        "category": "custom"
    }
}
```

---

## 添加新的故障注入脚本

### 1. 创建脚本目录

```bash
mkdir -p /project/data/data_scripts/new_fault
```

### 2. 创建注入脚本

```python
#!/usr/bin/env python3
"""新故障类型注入脚本"""
import subprocess
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end

print("=" * 60)
print("新故障类型注入")
print("=" * 60)

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")

mark_fault_start("new_fault", {"param": "value"})

# 执行故障注入逻辑
# ...

mark_fault_end("new_fault", {"result": "success"})

print("=" * 60)
print("故障注入完成")
print("=" * 60)
```

### 3. 更新配置

在 `unified_config.py` 的 `FAULT_CONFIG` 中添加配置。

---

## 故障标记验证

所有故障注入都会在 `/tmp/fault_markers.log` 中记录标记：

```bash
# 查看故障标记
cat /tmp/fault_markers.log | python3 -m json.tool

# 统计故障类型
python3 -c "
import json
from collections import Counter
with open('/tmp/fault_markers.log') as f:
    types = Counter(json.loads(line)['fault_type'] for line in f)
for t, c in types.items():
    print(f'{t}: {c}')
"
```

---

## 常见问题

### Q: 如何只执行特定的故障类型？

```bash
python3 collect_data/unified_scheduler_v2.py \
    --mode sequential \
    --sequence "data_skew:1,task_fail:1"
```

### Q: 如何修改故障参数？

通过环境变量传递：

```bash
LONG_TAIL_DURATION=120 TASK_FAIL_RATIO=0.5 \
    python3 collect_data/unified_scheduler_v2.py --mode sequential
```

### Q: 如何查看执行进度？

```bash
# 查看调度器日志
tail -f logs/scheduler_*.log

# 查看已收集的数据
ls -la collect_data/data/

# 查看状态
./batch_scheduler.sh status
```

### Q: 如何清理历史数据？

```bash
# 清理收集的数据
rm -rf collect_data/data/batch_*

# 清理日志
rm -rf logs/*

# 清理故障标记
rm /tmp/fault_markers.log
```

### Q: 如何重置Job ID从0000开始？

```bash
# 清空JobHistory
hdfs dfs -rm -r -skipTrash /tmp/hadoop-yarn/staging/history/done/*
hdfs dfs -rm -r -skipTrash /tmp/hadoop-yarn/staging/history/done_intermediate/*

# 重启YARN
$HADOOP_HOME/sbin/stop-yarn.sh
$HADOOP_HOME/sbin/start-yarn.sh
```

### Q: 批量调度支持SSH断开后继续运行吗？

是的，批量调度使用nohup后台运行，SSH断开后会继续执行。

---

## V3版本更新日志

1. **批次调度功能**：每批最多50次任务，自动分批
2. **独立批次目录**：每个批次创建独立文件夹
3. **fault_labels.csv**：自动生成，记录文件夹名与故障类型对应关系
4. **container_timestamp**：日志新增容器时间戳字段
5. **优化status显示**：美化输出，清晰展示执行进度和统计信息
6. **实时CSV写入**：每次执行后立即写入，不再批量写入
7. **batch_summary.json**：每个批次完成后生成摘要文件
