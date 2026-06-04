# Hadoop集群故障注入与数据收集系统 V3

## 项目概述

本项目用于在Hadoop集群中注入各种故障类型，并自动收集相关的日志和指标数据，用于故障检测研究。

**V3版本新特性：**
- **批次调度**: 支持大批量任务自动分批执行（每批最多50次）
- **独立批次目录**: 每个批次创建独立文件夹，便于数据分类整理
- **故障标签记录**: 自动生成fault_labels.csv，记录文件夹名与故障类型对应关系
- **容器时间戳**: 日志中新增container_timestamp字段，从container_id提取时间戳
- **优化状态显示**: 美化status命令输出，清晰展示执行进度和统计信息
- **后台运行**: 支持SSH断开后继续执行

## 环境要求

### 集群环境
- **Hadoop版本**: 3.4.1
- **操作系统**: Ubuntu 22.04
- **Python版本**: 3.10+
- **监控组件**: Prometheus + Grafana + Loki
- **HiBench**: /opt/HiBench

### 集群拓扑
| 节点 | 角色 | 服务 | IP |
|------|------|------|-----|
| cpf-1 | Master | NameNode, ResourceManager, HistoryServer | 10.10.3.183 |
| cpf-2 | Slave | DataNode, NodeManager | 10.10.1.96 |
| cpf-3 | Slave | DataNode, NodeManager | 10.10.3.222 |
| cpf-4 | Slave | DataNode, NodeManager | 10.10.0.176 |

### 依赖服务
- **Prometheus**: `http://localhost:9090` - 指标收集
- **Loki**: `http://localhost:3100` - 日志收集
- **HDFS NFS**: `/hdfs-nfs` - HDFS挂载点

## 项目结构

```
data_scripts/
├── batch_scheduler.sh               # 批量调度管理脚本（推荐使用）
├── collect_data/                    # 数据收集模块
│   ├── unified_scheduler_v2.py     # 主调度器
│   ├── scheduler_core.py           # 调度核心模块
│   ├── log_collector.py            # 日志收集模块
│   ├── metrics_collector.py        # 指标收集模块
│   ├── run_recorder.py             # 运行记录模块
│   ├── hibench_manager.py          # HiBench数据管理
│   ├── fault_marker.py             # 故障标记模块
│   ├── unified_config.py           # 统一配置
│   └── data/                       # 数据输出目录
│       └── batch_YYYYMMDD_HHMMSS/  # 批次目录
│           ├── execution_records.csv
│           ├── fault_labels.csv
│           ├── batch_summary.json
│           └── <fault_type>_<timestamp>/
│               ├── logs/
│               └── metrics/
├── common_mapreduce/                # 通用MapReduce脚本
├── data_skew/                       # 数据倾斜故障
├── data_bloat/                      # 数据膨胀故障
├── task_fail/                       # 任务失败故障
├── long_tail/                       # 长尾任务故障
├── wait_time/                       # 等待时间异常
├── run_time/                        # 运行时间异常
├── exit_time/                       # 退出时间异常
├── network_latency/                 # 网络延迟故障
├── docs/                            # 文档
│   ├── fault_classification.md     # 故障分类文档
│   ├── topology_guide.md           # 拓扑构建指南
│   └── supervised_learning_labels.md # 监督学习标签说明
└── logs/                            # 调度器日志
```

## 故障类型

| 故障类型 | 类别 | 注入位置 | 描述 | 标签 |
|---------|------|---------|------|------|
| **wordcount** | 正常 | - | 标准WordCount任务 | 0 |
| **data_skew** | 数据分布 | Mapper代码 | 分区倾斜 - 80%数据输出相同key | 4 |
| **data_bloat** | 数据分布 | Mapper代码 | 数据膨胀 - 生成10倍中间数据 | 5 |
| **task_fail** | 任务执行 | Mapper代码 | 任务失败 - 按task_id注入失败 | 6 |
| **long_tail** | 任务执行 | Mapper代码 | 长尾任务 - 按task_id注入延迟 | 7 |
| **wait_time** | 调度 | ResourceManager进程 | 等待时间异常 - 挂起RM 60秒 | 1 |
| **runtime_delta** | 调度 | MRAppMaster进程 | 运行时间异常 - 挂起AM 60秒 | 3 |
| **exit_time** | 节点管理 | NodeManager进程 | 退出时间异常 - 挂起NM 60秒 | 2 |
| **network_latency** | 网络 | 网络层(tc) | 网络延迟 - 注入100ms延迟 | 8 |

## 快速开始

### 使用批量调度脚本（推荐）

```bash
cd /project/data/data_scripts

# 启动批量调度（300次任务，自动分批）
TOTAL_RUNS=300 NORMAL_RATIO=0.3 FAULT_TYPES="wait_time,exit_time" ./batch_scheduler.sh start

# 查看状态
./batch_scheduler.sh status

# 停止调度
./batch_scheduler.sh stop
```

### 环境变量配置

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
```

## 输出数据格式

### 批次目录结构

```
collect_data/data/
└── batch_20260319_182810/           # 批次目录
    ├── execution_records.csv        # 执行记录
    ├── fault_labels.csv             # 故障标签（监督学习用）
    ├── batch_summary.json           # 批次摘要
    ├── wait_time_20260319_182810/   # 任务目录
    │   ├── logs/
    │   │   ├── cpf-1/
    │   │   │   ├── namenode_*.csv
    │   │   │   └── resourcemanager_*.csv
    │   │   ├── cpf-2/
    │   │   │   ├── datanode_*.csv
    │   │   │   ├── nodemanager_*.csv
    │   │   │   └── mapreduce_application_*_*.csv
    │   │   └── ...
    │   └── metrics/
    │       ├── cpf-1/
    │       │   ├── jvm_memory_*.csv
    │       │   ├── node_load*.csv
    │       │   └── ...
    │       └── ...
    ├── exit_time_20260319_183128/
    └── wordcount_20260319_183023/
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

### fault_labels.csv（监督学习标签）

| 字段 | 说明 |
|------|------|
| folder_name | 文件夹名（如wait_time_20260319_182810） |
| fault_type | 故障类型 |
| start_time | 开始时间 |
| end_time | 结束时间 |
| label | 故障标签（0=正常，1=wait_time，2=exit_time，...） |

### 日志CSV格式

| 字段 | 说明 |
|------|------|
| timestamp | 时间戳 |
| log_type | 日志类型（syslog/stdout/stderr等） |
| container_id | 容器ID |
| container_timestamp | 从container_id提取的时间戳 |
| node | 节点名 |
| application_id | Application ID |
| message | 日志内容 |

### 指标CSV格式

| 字段 | 说明 |
|------|------|
| timestamp | 时间戳 |
| timestamp_unix | Unix时间戳 |
| metric | 指标名 |
| category | 类别（cpu/memory/disk/network/hadoop/jvm） |
| hostname | 节点名 |
| service | 服务名 |
| value | 指标值 |

## 状态显示示例

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
```

## Python API使用

### 直接调用调度器

```bash
cd /project/data/data_scripts/collect_data

# 顺序模式
python3 unified_scheduler_v2.py \
    --mode sequential \
    --sequence "wait_time:2,exit_time:1,wordcount:1" \
    --data-size small \
    --interval-min 60 \
    --interval-max 120 \
    --max-batch-size 50

# 随机模式
python3 unified_scheduler_v2.py \
    --mode random \
    --fault-types wait_time exit_time \
    --fault-counts "wait_time:3,exit_time:2" \
    --normal-count 2 \
    --data-size small
```

### 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--mode` | 调度模式（sequential/random） | sequential |
| `--sequence` | 执行序列，格式：fault_type:count,... | - |
| `--data-size` | 数据规模（tiny/small/large/huge/gigantic） | tiny |
| `--interval-min` | 最小间隔（秒） | 60 |
| `--interval-max` | 最大间隔（秒） | 300 |
| `--max-batch-size` | 每批次最大任务数 | 50 |
| `--output-dir` | 输出目录 | collect_data/data |

## 数据规模说明

| 规模 | 大小 | 用途 |
|------|------|------|
| tiny | ~32MB | 快速验证 |
| small | ~320MB | 开发测试 |
| large | ~32GB | 性能测试 |
| huge | ~320GB | 压力测试 |
| gigantic | ~3.2TB | 极限测试 |

## 架构说明

### 解耦架构

```
┌─────────────────────────────────────────────────────────────┐
│                    unified_scheduler_v2                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ FaultScheduler│  │ LogCollector │  │ MetricsCollector   │ │
│  │  (调度核心)   │  │ (日志收集)   │  │   (指标收集)        │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
│  ┌─────────────┐  ┌─────────────┐                          │
│  │ CSV记录器    │  │HiBenchManager│                          │
│  │ (实时写入)   │  │ (数据管理)   │                          │
│  └─────────────┘  └─────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

### 模块职责

| 模块 | 职责 |
|------|------|
| `scheduler_core.py` | 调度逻辑、序列生成、间隔控制 |
| `log_collector.py` | 从Loki收集日志、从HDFS NFS收集MapReduce日志 |
| `metrics_collector.py` | 从Prometheus收集指标 |
| `run_recorder.py` | 记录执行信息 |
| `hibench_manager.py` | 管理HiBench数据生成 |
| `unified_config.py` | 统一配置管理 |

## 配置说明

编辑 `collect_data/unified_config.py` 修改：

- `PROMETHEUS_API`: Prometheus API地址
- `LOKI_API`: Loki API地址
- `HDFS_NFS_MOUNT`: HDFS NFS挂载点
- `HADOOP_HOME`: Hadoop安装路径
- `TOPOLOGY`: 集群拓扑配置
- `FAULT_CONFIG`: 故障类型配置

## 依赖安装

```bash
pip install requests pandas numpy
```

## 注意事项

1. 需要SSH免密登录到所有节点
2. 需要sudo权限执行部分故障注入脚本
3. 确保Prometheus和Loki服务正常运行
4. 确保HiBench已正确安装在 `/opt/HiBench`
5. 首次使用需要生成测试数据，会自动调用HiBench prepare脚本
6. 批量调度支持SSH断开后继续运行（使用nohup）

## 相关文档

- [故障分类与推荐](docs/fault_classification.md)
- [拓扑数据构建指南](docs/topology_guide.md)
- [监督学习标签说明](docs/supervised_learning_labels.md)
- [调度器使用指南](docs/scheduler_usage_guide.md)

## 版本历史

- **V3.0** - 当前版本
  - 批次调度功能（每批最多50次）
  - 独立批次目录
  - fault_labels.csv自动生成
  - container_timestamp字段
  - 优化status显示
  - 后台运行支持

- **V2.0**
  - 解耦架构设计
  - HiBench数据规模集成
  - 双调度模式（顺序/随机）
  - 随机间隔调度
