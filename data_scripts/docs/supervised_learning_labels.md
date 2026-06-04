# 监督学习标签映射文档 V3

## 故障类型与标签映射

| 标签 | 故障类型 | 描述 | 注入方式 |
|-----|---------|------|---------|
| 0 | normal/wordcount | 正常任务 | 无故障 |
| 1 | wait_time | 等待时间异常 | 挂起ResourceManager |
| 2 | exit_time | 退出时间异常 | 挂起NodeManager |
| 3 | runtime_delta | 运行时间异常 | 挂起MRAppMaster |
| 4 | data_skew | 数据倾斜 | Mapper输出倾斜 |
| 5 | data_bloat | 数据膨胀 | Mapper输出膨胀 |
| 6 | task_fail | 任务失败 | Mapper抛出异常 |
| 7 | long_tail | 长尾任务 | Mapper延迟 |
| 8 | network_latency | 网络延迟 | tc命令注入延迟 |

---

## V3版本数据目录结构

```
/project/data/data_scripts/collect_data/data/
└── batch_20260319_182810/           # 批次目录（每批最多50次）
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

---

## fault_labels.csv 格式（V3更新）

```csv
folder_name,fault_type,start_time,end_time,label
wait_time_20260319_182810,wait_time,2026-03-19T18:28:10,2026-03-19T18:30:00,1
wordcount_20260319_183023,wordcount,2026-03-19T18:30:23,2026-03-19T18:31:18,0
exit_time_20260319_183128,exit_time,2026-03-19T18:31:28,2026-03-19T18:33:29,2
wait_time_20260319_183357,wait_time,2026-03-19T18:33:57,2026-03-19T18:37:18,1
```

**字段说明：**

| 字段 | 说明 |
|------|------|
| folder_name | 文件夹名（如wait_time_20260319_182810） |
| fault_type | 故障类型 |
| start_time | 开始时间（ISO格式） |
| end_time | 结束时间（ISO格式） |
| label | 故障标签（0-8） |

---

## execution_records.csv 格式（V3更新）

```csv
seq_idx,fault_type,start_time,end_time,duration,success,job_id,application_id,logs_dir,metrics_dir,error,nodes,method,task_dir
1,wait_time,2026-03-19T18:28:10,2026-03-19T18:30:00,110,True,,application_1773944500533_0002,/path/logs,/path/metrics,,cpf-1,挂起ResourceManager,/path/task_dir
2,wordcount,2026-03-19T18:30:23,2026-03-19T18:31:18,55,True,,application_1773944500533_0003,/path/logs,/path/metrics,,,,标准WordCount实现,/path/task_dir
```

---

## 核心指标说明

用于故障检测的5个核心指标：

| 指标名 | 计算公式 | 反映的故障特征 |
|--------|---------|---------------|
| jvm_heap_used_ratio | jvm_heap_used / jvm_heap_max | JVM堆内存使用率，反映GC压力和内存问题 |
| container_running | Hadoop_NM_ContainersRunning | 容器运行数量，反映NM状态和任务分布 |
| container_failed | Hadoop_NM_ContainersFailed | 容器失败数量，反映任务执行异常 |
| cpu_usage | rate(node_cpu_seconds_total[1m]) | CPU使用率，反映节点负载 |
| memory_usage | 1 - MemAvailable/MemTotal | 内存使用率，反映内存压力 |

---

## 日志CSV格式（V3更新）

```csv
timestamp,log_type,container_id,container_timestamp,node,application_id,message
2026-03-19 18:28:15,syslog,container_1772798254657_0286_20260319_163759,2026-03-19 16:37:59,cpf-2,application_1773944500533_0002,Container started...
```

**新增字段：**

| 字段 | 说明 |
|------|------|
| container_timestamp | 从container_id提取的时间戳，格式：YYYY-MM-DD HH:MM:SS |

container_id格式解析：
- `container_1772798254657_0286_20260319_163759`
- 最后的`_20260319_163759`表示容器创建时间：2026-03-19 16:37:59

---

## 指标CSV格式

```csv
timestamp,timestamp_unix,metric,category,hostname,service,value
2026-03-19 18:28:00,1710832880,jvm_memory_bytes_used,jvm,cpf-1,namenode,1.5e+09
2026-03-19 18:28:00,1710832880,node_load1,cpu,cpf-2,node_exporter,2.5
```

---

## 特征向量构建

### 向量维度 (5指标 × 4节点 = 20维)

```
[cpf-1.jvm_heap, cpf-1.container_running, cpf-1.container_failed, cpf-1.cpu_usage, cpf-1.memory_usage,
 cpf-2.jvm_heap, cpf-2.container_running, cpf-2.container_failed, cpf-2.cpu_usage, cpf-2.memory_usage,
 cpf-3.jvm_heap, cpf-3.container_running, cpf-3.container_failed, cpf-3.cpu_usage, cpf-3.memory_usage,
 cpf-4.jvm_heap, cpf-4.container_running, cpf-4.container_failed, cpf-4.cpu_usage, cpf-4.memory_usage]
```

### 标签向量（One-Hot编码）

```
正常: [1, 0, 0, 0, 0, 0, 0, 0, 0]  (标签=0)
wait_time: [0, 1, 0, 0, 0, 0, 0, 0, 0]  (标签=1)
exit_time: [0, 0, 1, 0, 0, 0, 0, 0, 0]  (标签=2)
...
```

---

## 批量运行配置

### 参数配置

| 参数 | 值 | 说明 |
|------|-----|------|
| 总任务数 | 300 | 目标运行次数 |
| 每批次上限 | 50 | 自动分批 |
| 正常比例 | 30% (90次) | 无故障基准 |
| wait_time | 35% (105次) | 挂起RM |
| exit_time | 35% (105次) | 挂起NM |
| 间隔时间 | 60-120秒 | 随机间隔 |
| 数据大小 | small | ~320MB |

### 启动命令

```bash
cd /project/data/data_scripts

# 使用默认配置
./batch_scheduler.sh start

# 自定义配置
TOTAL_RUNS=300 NORMAL_RATIO=0.3 FAULT_TYPES="wait_time,exit_time" \
INTERVAL_MIN=60 INTERVAL_MAX=120 DATA_SIZE=small MAX_BATCH_SIZE=50 \
./batch_scheduler.sh start
```

### 查看状态

```bash
./batch_scheduler.sh status
```

### 停止命令

```bash
./batch_scheduler.sh stop
```

---

## 磁盘空间预估

| 项目 | 大小 | 说明 |
|------|------|------|
| 每次任务 | ~7MB | 5个核心指标 × 4节点 × 时间序列 |
| 300次任务 | ~2.1GB | 估算最大值 |
| 可用空间 | ~20GB | 当前剩余 |
| 安全余量 | 5GB | 预留空间 |

结论：磁盘空间充足，可以运行300次任务。

---

## 数据加载示例

### Python加载标签数据

```python
import pandas as pd
from pathlib import Path

def load_batch_labels(data_dir: str):
    """加载所有批次的标签数据"""
    all_labels = []
    data_path = Path(data_dir)
    
    for batch_dir in data_path.glob("batch_*"):
        labels_file = batch_dir / "fault_labels.csv"
        if labels_file.exists():
            df = pd.read_csv(labels_file)
            df['batch'] = batch_dir.name
            all_labels.append(df)
    
    return pd.concat(all_labels, ignore_index=True)

def load_execution_records(data_dir: str):
    """加载所有批次的执行记录"""
    all_records = []
    data_path = Path(data_dir)
    
    for batch_dir in data_path.glob("batch_*"):
        records_file = batch_dir / "execution_records.csv"
        if records_file.exists():
            df = pd.read_csv(records_file)
            df['batch'] = batch_dir.name
            all_records.append(df)
    
    return pd.concat(all_records, ignore_index=True)

# 使用示例
data_dir = "/project/data/data_scripts/collect_data/data"
labels_df = load_batch_labels(data_dir)
records_df = load_execution_records(data_dir)

print(f"总任务数: {len(labels_df)}")
print(f"标签分布:\n{labels_df['label'].value_counts().sort_index()}")
print(f"故障类型分布:\n{labels_df['fault_type'].value_counts()}")
```

### 统计标签分布

```python
from collections import Counter

def get_label_distribution(df):
    """获取标签分布"""
    return Counter(df['label'])

def filter_by_label(df, label):
    """按标签筛选"""
    return df[df['label'] == label]

def filter_by_fault_type(df, fault_type):
    """按故障类型筛选"""
    return df[df['fault_type'] == fault_type]

# 使用示例
dist = get_label_distribution(labels_df)
print("标签分布:")
for label, count in sorted(dist.items()):
    print(f"  标签 {label}: {count} 次")
```

---

## 故障特征总结

| 故障类型 | 核心指标变化特征 |
|---------|----------------|
| normal | 所有指标平稳 |
| wait_time | container_running下降，cpu_usage平稳 |
| exit_time | container_running=0或下降，container_failed可能增加 |
| runtime_delta | cpu_usage下降（AM被挂起） |
| data_skew | container_running不均衡，某些节点偏高 |
| data_bloat | cpu_usage升高，memory_usage升高 |
| task_fail | container_failed增加 |
| long_tail | cpu_usage局部升高，某些任务时间长 |
| network_latency | container_running可能下降 |

---

## 数据清洗建议

1. **删除无效数据**：过滤掉指标值为0或异常的记录
2. **统一时间戳**：将所有指标对齐到相同时间点
3. **归一化处理**：将不同量纲的指标归一化到[0,1]
4. **窗口采样**：按固定窗口采样形成时序特征
5. **批次分离**：不同批次的数据分开处理，避免混淆

---

## V3版本更新日志

1. **批次目录结构**：每个批次独立目录，最多50次任务
2. **fault_labels.csv**：新增folder_name字段，记录文件夹名
3. **container_timestamp**：日志新增容器时间戳字段
4. **实时写入**：每次执行后立即写入CSV，不再批量写入
5. **batch_summary.json**：每个批次完成后生成摘要文件
