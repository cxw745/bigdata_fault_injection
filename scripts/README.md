# 大数据故障注入系统 (Big Data Fault Injection System)

一个用于大数据集群故障检测研究的故障注入和数据收集系统，支持多种故障类型的模拟和监控数据采集。

## 📋 目录

- [项目概述](#项目概述)
- [功能特性](#功能特性)
- [系统架构](#系统架构)
- [故障类型](#故障类型)
- [快速开始](#快速开始)
- [使用指南](#使用指南)
- [目录结构](#目录结构)
- [配置说明](#配置说明)
- [故障分类](#故障分类)
- [常见问题](#常见问题)
- [贡献指南](#贡献指南)

## 🎯 项目概述

本项目是一个面向大数据集群（Hadoop/YARN）的故障注入和数据收集系统，旨在为大数据故障检测研究提供实验平台。系统支持多种故障类型的模拟，并能够自动收集故障前后的系统指标、日志和拓扑信息。

### 应用场景

- 大数据故障检测算法研究
- 故障诊断模型训练数据收集
- 系统容错能力评估
- 异常检测算法验证
- AIOps智能运维研究

## ✨ 功能特性

### 故障注入

- ✅ **数据级故障**：数据膨胀、数据倾斜
- ✅ **任务级故障**：任务失败、长尾任务、慢任务
- ✅ **进程级故障**：等待时间异常、运行时间异常、退出时间异常
- ✅ **可配置故障参数**：支持自定义故障强度、持续时间等

### 数据收集

- 📊 **系统指标**：CPU、内存、磁盘、网络等
- 📝 **日志收集**：YARN日志、Hadoop日志、应用日志
- 🌐 **拓扑信息**：集群拓扑、任务拓扑
- ⏱️ **时间窗口**：故障前后的完整数据采集

### 自动化

- 🤖 **批处理支持**：支持批量故障注入测试
- 📅 **定时调度**：支持定时故障注入
- 🔄 **自动恢复**：故障注入后自动恢复系统状态
- 📈 **数据汇总**：自动生成测试报告

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                   大数据故障注入系统                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ 故障注入器   │  │ 数据收集器   │  │ 调度管理器   │ │
│  ├──────────────┤  ├──────────────┤  ├──────────────┤ │
│  │ • 数据故障   │  │ • 指标收集   │  │ • 批处理     │ │
│  │ • 任务故障   │  │ • 日志收集   │  │ • 定时调度   │ │
│  │ • 进程故障   │  │ • 拓扑收集   │  │ • 状态监控   │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘ │
│         │                 │                 │          │
└─────────┼─────────────────┼─────────────────┼──────────┘
          │                 │                 │
          ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────┐
│                   Hadoop/YARN 集群                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐│
│  │ResourceManager│  │NodeManager│  │MRAppMaster│  │Task     ││
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘│
└─────────────────────────────────────────────────────────┘
```

## 🔧 故障类型

### 核心故障（高优先级）

| 故障类型 | 层次 | 说明 | 实现方式 |
|---------|------|------|---------|
| **data_skew** | 数据级 | 数据倾斜 | 特定key集中到某些reducer |
| **data_bloat** | 数据级 | 数据膨胀 | 每行复制多份 |
| **task_fail** | 任务级 | 任务失败 | 随机抛出异常 |
| **long_tail** | 任务级 | 长尾任务 | 挂起部分task进程 |

### 扩展故障（中优先级）

| 故障类型 | 层次 | 说明 | 实现方式 |
|---------|------|------|---------|
| **wait_time** | 进程级 | 等待时间异常 | 挂起ResourceManager |
| **runtime_delta** | 进程级 | 运行时间异常 | 挂起MRAppMaster |
| **exit_time** | 进程级 | 退出时间异常 | 挂起NodeManager |
| **slow_task** | 任务级 | 慢任务 | 挂起单个task |

详细的故障分类说明请参考 [故障分类说明.md](file:///scripts/故障分类说明.md)

## 🚀 快速开始

### 环境要求

- **操作系统**：Linux (Ubuntu/CentOS)
- **Hadoop**：2.x 或 3.x
- **Python**：3.6+
- **集群**：至少3个节点（1个ResourceManager + 2个NodeManager）

### 安装步骤

1. **克隆仓库**
```bash
git clone <repository-url>
cd scripts
```

2. **配置集群信息**
```bash
# 编辑配置文件
vim collect_data/config.py
```

3. **测试故障注入**
```bash
# 测试数据膨胀故障
cd /scripts/data_bloat
./inject_data_bloat.sh

# 测试任务失败故障
cd /scripts/task_fail
./inject_task_fail.sh
```

4. **运行数据收集**
```bash
# 收集故障数据
python3 /scripts/collect_data/run_fault_test.py \
  --fault data_skew \
  --output-dir /tmp/fault_test_results \
  --minutes-before 3 \
  --minutes-after 5
```

## 📖 使用指南

### 单次故障注入

#### 数据膨胀故障
```bash
cd /scripts/data_bloat
./inject_data_bloat.sh
```

#### 数据倾斜故障
```bash
cd /scripts/data_skew
./inject_data_skew.sh
```

#### 任务失败故障
```bash
cd /scripts/task_fail
./inject_task_fail.sh
```

#### 长尾任务故障
```bash
cd /scripts/long_tail
./inject_long_tail.sh
```

### 进程级故障注入

#### 等待时间异常（挂起ResourceManager）
```bash
python3 /scripts/wait_time/inject_wait_time.py
```

#### 运行时间异常（挂起MRAppMaster）
```bash
python3 /scripts/run_time/inject_run_time.py
```

#### 退出时间异常（挂起NodeManager）
```bash
python3 /scripts/exit_time/inject_exit_time.py
```

### 批量故障测试

1. **配置故障列表**
```bash
vim /scripts/collect_data/fault_config.json
```

2. **启动批量测试**
```bash
cd /scripts
./start_batch_fault.sh
```

3. **查看测试状态**
```bash
./status_batch_fault.sh
```

4. **停止批量测试**
```bash
./stop_batch_fault.sh
```

### 数据收集

#### 收集单个故障数据
```bash
python3 /scripts/collect_data/run_fault_test.py \
  --fault <fault_type> \
  --output-dir <output_directory> \
  --minutes-before <minutes> \
  --minutes-after <minutes>
```

**参数说明：**
- `--fault`: 故障类型（data_skew, data_bloat, task_fail等）
- `--output-dir`: 输出目录
- `--minutes-before`: 故障前采集时间（分钟）
- `--minutes-after`: 故障后采集时间（分钟）

#### 收集所有指标
```bash
python3 /scripts/collect_data/collect_data.py \
  --output-dir /tmp/collect_results \
  --duration 10
```

## 📁 目录结构

```
scripts/
├── collect_data/              # 数据收集模块
│   ├── collect_data.py        # 数据收集主程序
│   ├── collect_metrics.py    # 系统指标收集
│   ├── collect_logs.py        # 日志收集
│   ├── collect_topology.py    # 拓扑信息收集
│   ├── run_fault_test.py      # 故障测试运行器
│   ├── fault_config.json      # 故障配置文件
│   └── config.py              # 集群配置
├── data_bloat/                # 数据膨胀故障
│   ├── inject_data_bloat.sh   # 故障注入脚本
│   ├── mapper_bloat.py       # Mapper脚本
│   └── bloat_generator_3.2gb.py  # 数据生成器
├── data_skew/                 # 数据倾斜故障
│   ├── inject_data_skew.sh    # 故障注入脚本
│   ├── skew_mapper.py        # Mapper脚本
│   ├── skew_reducer.py       # Reducer脚本
│   └── skew_generator_3.2gb.py  # 数据生成器
├── task_fail/                 # 任务失败故障
│   ├── inject_task_fail.sh   # 故障注入脚本
│   ├── mapper_task_fail.py   # Mapper脚本
│   └── reducer_task_fail.py  # Reducer脚本
├── long_tail/                 # 长尾任务故障
│   ├── inject_long_tail.sh   # 故障注入脚本
│   ├── mapper_long_tail.py   # Mapper脚本
│   └── reducer_long_tail.py  # Reducer脚本
├── wait_time/                 # 等待时间故障
│   └── inject_wait_time.py   # 故障注入脚本
├── run_time/                  # 运行时间故障
│   └── inject_run_time.py     # 故障注入脚本
├── exit_time/                 # 退出时间故障
│   └── inject_exit_time.py    # 故障注入脚本
├── common_mapreduce/          # 通用MapReduce脚本
│   ├── mapper.py             # 通用Mapper
│   ├── reducer.py            # 通用Reducer
│   └── run.sh                # 运行脚本
├── logs/                      # 日志目录
├── run_inject_fault.sh        # 故障注入主脚本
├── stop_inject_fault.sh       # 停止故障注入
├── start_batch_fault.sh       # 启动批量测试
├── stop_batch_fault.sh        # 停止批量测试
├── status_batch_fault.sh      # 查看批量测试状态
├── scheduler.py               # 调度器
├── 故障分类说明.md             # 故障分类详细说明
└── README.md                  # 本文档
```

## ⚙️ 配置说明

### 集群配置

编辑 `collect_data/config.py` 文件：

```python
# 集群节点配置
CLUSTER_NODES = {
    "cpf-1": "192.168.1.1",  # ResourceManager
    "cpf-2": "192.168.1.2",  # NodeManager
    "cpf-3": "192.168.1.3",  # NodeManager
    "cpf-4": "192.168.1.4",  # NodeManager
}

# Hadoop配置
HADOOP_HOME = "/opt/hadoop"
HDFS_USER = "hadoop"
```

### 故障配置

编辑 `collect_data/fault_config.json` 文件：

```json
{
    "faults": [
        {
            "type": "data_skew",
            "count": 2,
            "order": 1,
            "enabled": true,
            "description": "数据倾斜故障",
            "params": {
                "minutes_before": 3,
                "minutes_after": 5,
                "wait_minutes": 1
            }
        }
    ],
    "global_settings": {
        "output_dir": "/tmp/fault_test_results",
        "random_order": false,
        "interval_between_faults": 120,
        "interval_between_runs": 300
    }
}
```

## 📊 故障分类

系统将故障按影响范围分为7个层次：

1. **主机级故障** - 影响整个节点
2. **进程级故障** - 影响特定Hadoop/YARN进程
3. **任务级故障** - 影响单个或多个MapReduce任务
4. **数据级故障** - 影响数据处理和分布
5. **配置级故障** - 影响资源配置和调度
6. **调度级故障** - 影响任务调度
7. **统计级故障** - 基于历史数据的统计异常

详细的故障分类说明请参考 [故障分类说明.md](file:///scripts/故障分类说明.md)

## ❓ 常见问题

### Q1: 故障注入后系统没有恢复正常？

**A:** 检查故障注入脚本是否正确执行，可以手动运行恢复脚本：
```bash
./stop_inject_fault.sh
```

### Q2: 数据收集失败？

**A:** 检查以下几点：
1. 确认集群节点SSH免密登录配置正确
2. 确认Hadoop服务正常运行
3. 检查输出目录权限

### Q3: 如何自定义故障参数？

**A:** 编辑对应故障注入脚本中的参数，例如：
```python
# 在 wait_time/inject_wait_time.py 中
FAULT_DURATION = 120  # 修改故障持续时间
```

### Q4: 批量测试失败？

**A:** 检查故障配置文件是否正确，确保：
1. 故障类型拼写正确
2. 参数格式正确
3. 输出目录存在且有写权限

### Q5: 如何添加新的故障类型？

**A:** 参考现有故障类型的实现：
1. 创建新的故障目录
2. 编写故障注入脚本
3. 在配置文件中添加故障定义
4. 测试故障注入效果

## 🤝 贡献指南

欢迎贡献代码、报告问题或提出建议！

### 贡献流程

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

### 代码规范

- Python代码遵循 PEP 8 规范
- Shell脚本使用 ShellCheck 检查
- 添加适当的注释和文档
- 保持代码简洁和可读性

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 📞 联系方式

- 项目主页：[GitHub Repository]
- 问题反馈：[GitHub Issues]
- 邮箱：your-email@example.com

## 🙏 致谢

感谢所有为本项目做出贡献的开发者！

---

**注意：** 本系统仅用于研究和测试目的，在生产环境中使用请谨慎评估风险。
