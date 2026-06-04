# 故障类型分类与实现指南 V3

## 一、已实现故障类型概览

当前系统已实现 **9种** 故障类型，覆盖数据分布、任务执行、调度、节点管理和网络5大类。

### 故障类型总览

| 故障类型 | 分类 | 注入方式 | 影响范围 | 标签 |
|---------|------|---------|---------|------|
| wordcount | 基准 | 标准WordCount | 无故障 | 0 |
| data_skew | 数据分布 | Mapper代码倾斜 | 单个Reducer | 4 |
| data_bloat | 数据分布 | Mapper输出膨胀 | 网络和磁盘 | 5 |
| task_fail | 任务执行 | Mapper异常抛出 | 任务重试 | 6 |
| long_tail | 任务执行 | Mapper延迟注入 | 任务时长 | 7 |
| wait_time | 调度 | RM进程挂起 | 全局调度 | 1 |
| runtime_delta | 调度 | AM进程挂起 | 单任务 | 3 |
| exit_time | 节点管理 | NM进程挂起 | 单节点容器 | 2 |
| network_latency | 网络 | tc命令延迟 | 网络传输 | 8 |

---

## 二、已实现故障详细说明

### 1. 数据分布类故障

#### data_skew - 数据倾斜

| 属性 | 值 |
|-----|-----|
| **注入位置** | Mapper代码 |
| **实现原理** | 80%数据输出相同key，导致单个Reducer处理大部分数据 |
| **预期效果** | 单个Reducer执行时间远超其他Reducer |
| **检测指标** | reduce任务执行时间差异、shuffle字节数分布 |
| **配置参数** | `SKEW_RATIO=0.8` (倾斜比例) |
| **使用示例** | `python3 data_skew/inject_data_skew.py` |

#### data_bloat - 数据膨胀

| 属性 | 值 |
|-----|-----|
| **注入位置** | Mapper代码 |
| **实现原理** | 每个输入记录输出多份，导致中间数据膨胀 |
| **预期效果** | Map输出数据量急剧增加，shuffle压力大 |
| **检测指标** | map输出字节数、shuffle字节数、磁盘IO |
| **配置参数** | `BLOAT_FACTOR=20` (膨胀倍数) |
| **使用示例** | `BLOAT_FACTOR=10 python3 data_bloat/inject_data_bloat.py` |

---

### 2. 任务执行类故障

#### task_fail - 任务失败

| 属性 | 值 |
|-----|-----|
| **注入位置** | Mapper代码 |
| **实现原理** | 指定任务抛出RuntimeException |
| **预期效果** | 任务失败并自动重试 |
| **检测指标** | 失败任务数、重试次数 |
| **配置参数** | `TASK_FAIL_MODE=ratio`, `TASK_FAIL_RATIO=0.8` |
| **使用示例** | `TASK_FAIL_RATIO=0.5 python3 task_fail/inject_task_fail.py` |

**注入模式**:
- `task_id`: 指定任务ID注入（确定性）
- `ratio`: 按比例注入

#### long_tail - 长尾任务

| 属性 | 值 |
|-----|-----|
| **注入位置** | Mapper代码 |
| **实现原理** | 指定任务注入延迟 |
| **预期效果** | 部分任务执行时间变长，拖慢整体进度 |
| **检测指标** | 任务执行时间分布、最大/平均时间比 |
| **配置参数** | `LONG_TAIL_DURATION=60`, `LONG_TAIL_TASK_IDS=0,5,10` |
| **使用示例** | `LONG_TAIL_DURATION=120 python3 long_tail/inject_long_tail.py` |

---

### 3. 调度类故障

#### wait_time - 等待时间异常

| 属性 | 值 |
|-----|-----|
| **注入位置** | ResourceManager进程 (cpf-1) |
| **实现原理** | SIGSTOP挂起RM进程 |
| **预期效果** | 任务无法被调度，等待时间增加 |
| **检测指标** | 任务等待时间、调度延迟 |
| **配置参数** | `FAULT_DURATION=120` (挂起时长) |
| **使用示例** | `python3 wait_time/inject_wait_time.py` |

#### runtime_delta - 运行时间异常

| 属性 | 值 |
|-----|-----|
| **注入位置** | MRAppMaster进程 |
| **实现原理** | SIGSTOP挂起AM进程 |
| **预期效果** | 任务心跳中断，运行时间增加 |
| **检测指标** | AM心跳间隔、任务运行时间 |
| **配置参数** | `FAULT_DURATION=120` (挂起时长) |
| **使用示例** | `python3 run_time/inject_runtime_delta.py` |

---

### 4. 节点管理类故障

#### exit_time - 退出时间异常

| 属性 | 值 |
|-----|-----|
| **注入位置** | NodeManager进程 (cpf-2, cpf-3, cpf-4) |
| **实现原理** | SIGSTOP挂起NM进程 |
| **预期效果** | 容器无法正常退出，任务完成时间变长 |
| **检测指标** | NM心跳超时、容器状态 |
| **配置参数** | `FAULT_DURATION=60` (挂起时长) |
| **使用示例** | `python3 exit_time/inject_exit_time.py` |

---

### 5. 网络类故障

#### network_latency - 网络延迟

| 属性 | 值 |
|-----|-----|
| **注入位置** | 网络层 (使用chaosblade) |
| **实现原理** | tc命令注入网络延迟 |
| **预期效果** | 网络传输延迟增加 |
| **检测指标** | 网络延迟、TCP重传、shuffle时间 |
| **配置参数** | `LATENCY_MS=500`, `FAULT_DURATION=60` |
| **使用示例** | `python3 network_latency/inject_network_latency.py` |

---

## 三、推荐新增故障类型

以下故障类型建议后续实现，以扩展故障覆盖范围。

### 1. 资源限制类 (Resource)

#### memory_pressure - 内存压力故障

| 属性 | 建议值 |
|-----|-------|
| **优先级** | ⭐⭐⭐ 高 |
| **注入位置** | 容器配置/YARN配置 |
| **实现原理** | 使用cgroups限制容器内存，或通过stress-ng消耗内存 |
| **预期效果** | 触发频繁GC，可能出现OOM |
| **检测指标** | JVM GC次数/时间、内存使用率、OOM次数 |
| **实现难度** | ⭐⭐⭐ |

**实现建议**:
```bash
# 方式1: 使用cgroups限制内存
echo 536870912 > /sys/fs/cgroup/memory/hadoop-yarn/memory.limit_in_bytes

# 方式2: 使用stress-ng
stress-ng --vm 4 --vm-bytes 80%
```

#### cpu_contention - CPU竞争故障

| 属性 | 建议值 |
|-----|-------|
| **优先级** | ⭐⭐⭐ 高 |
| **注入位置** | 容器cgroups |
| **实现原理** | 限制容器CPU配额(cfs_quota_us) |
| **预期效果** | 任务执行时间增加，CPU throttling |
| **检测指标** | CPU使用率、CPU throttling次数、任务执行时间 |
| **实现难度** | ⭐⭐ |

**实现建议**:
```bash
# 限制CPU使用率为50%
echo 50000 > /sys/fs/cgroup/cpu/hadoop-yarn/cpu.cfs_quota_us
echo 100000 > /sys/fs/cgroup/cpu/hadoop-yarn/cpu.cfs_period_us
```

#### container_oom - 容器OOM故障

| 属性 | 建议值 |
|-----|-------|
| **优先级** | ⭐⭐ 中 |
| **注入位置** | Mapper代码 |
| **实现原理** | 在Mapper中申请大量内存触发OOM |
| **预期效果** | 容器被kill，任务失败重试 |
| **检测指标** | 内存使用率、OOM kill次数、容器失败数 |
| **实现难度** | ⭐ |

**实现建议**:
```python
# mapper_oom.py
import sys
def inject_oom():
    # 申请大量内存触发OOM
    data = []
    while True:
        data.append(' ' * 1024 * 1024)  # 每次申请1MB
```

---

### 2. 存储类故障 (Storage)

#### disk_io_slow - 磁盘IO缓慢

| 属性 | 建议值 |
|-----|-------|
| **优先级** | ⭐⭐⭐ 高 |
| **注入位置** | 设备层 |
| **实现原理** | 使用blkio cgroups或device mapper限制IO带宽 |
| **预期效果** | 数据读写延迟增加，shuffle变慢 |
| **检测指标** | 磁盘IO延迟、IOPS、吞吐量 |
| **实现难度** | ⭐⭐⭐ |

**实现建议**:
```bash
# 使用blkio限制IO
echo "8:0 1048576" > /sys/fs/cgroup/blkio/hadoop-yarn/blkio.throttle.read_bps_device
echo "8:0 1048576" > /sys/fs/cgroup/blkio/hadoop-yarn/blkio.throttle.write_bps_device
```

#### namenode_slow - NameNode响应缓慢

| 属性 | 建议值 |
|-----|-------|
| **优先级** | ⭐⭐ 中 |
| **注入位置** | NameNode进程 (cpf-1) |
| **实现原理** | SIGSTOP挂起NN或限制其CPU |
| **预期效果** | HDFS元数据操作延迟增加 |
| **检测指标** | NN RPC延迟、RPC队列长度 |
| **实现难度** | ⭐⭐ |

**实现建议**:
```bash
# 挂起NameNode
kill -STOP $(pgrep -f "org.apache.hadoop.hdfs.server.namenode.NameNode")
# 恢复
kill -CONT $(pgrep -f "org.apache.hadoop.hdfs.server.namenode.NameNode")
```

---

### 3. 网络类故障 (Network)

#### shuffle_failure - Shuffle失败

| 属性 | 建议值 |
|-----|-------|
| **优先级** | ⭐⭐⭐ 高 |
| **注入位置** | 网络层/NodeManager |
| **实现原理** | 使用iptables丢弃shuffle端口数据包 |
| **预期效果** | Shuffle阶段失败，任务重试 |
| **检测指标** | Shuffle失败次数、网络丢包率 |
| **实现难度** | ⭐⭐⭐ |

**实现建议**:
```bash
# 丢弃shuffle端口(8088)的部分数据包
iptables -A INPUT -p tcp --dport 8088 -m statistic --mode random --probability 0.1 -j DROP
```

#### rpc_timeout - RPC超时

| 属性 | 建议值 |
|-----|-------|
| **优先级** | ⭐⭐ 中 |
| **注入位置** | 网络层 |
| **实现原理** | 使用tc延迟特定端口的RPC调用 |
| **预期效果** | RPC超时，任务失败或重试 |
| **检测指标** | RPC延迟、超时次数 |
| **实现难度** | ⭐⭐⭐ |

---

### 4. 配置类故障 (Configuration)

#### misconfiguration - 配置错误

| 属性 | 建议值 |
|-----|-------|
| **优先级** | ⭐ 低 |
| **注入位置** | Hadoop配置文件 |
| **实现原理** | 修改关键配置参数 |
| **预期效果** | 资源分配不合理，任务失败或性能下降 |
| **检测指标** | 配置变更、任务失败率 |
| **实现难度** | ⭐⭐ |

---

## 四、故障类型选择指南

### 按故障场景选择

| 场景 | 推荐故障类型 |
|-----|-------------|
| 数据倾斜检测 | data_skew, data_bloat |
| 任务执行异常 | task_fail, long_tail, container_oom |
| 调度延迟 | wait_time, runtime_delta |
| 节点故障 | exit_time, namenode_slow |
| 网络问题 | network_latency, shuffle_failure |
| 资源竞争 | memory_pressure, cpu_contention, disk_io_slow |

### 按检测难度选择

| 难度 | 故障类型 |
|-----|---------|
| 容易检测 | task_fail, container_oom, network_latency |
| 中等难度 | data_skew, long_tail, exit_time |
| 较难检测 | wait_time, runtime_delta, cpu_contention |

### 按实现优先级选择

| 优先级 | 故障类型 | 理由 |
|-------|---------|------|
| 高 | memory_pressure, cpu_contention | 资源类故障常见且重要 |
| 高 | disk_io_slow | 存储瓶颈常见 |
| 高 | shuffle_failure | MapReduce特有故障 |
| 中 | namenode_slow, container_oom | 特定场景重要 |
| 中 | rpc_timeout | 网络类扩展 |
| 低 | misconfiguration | 配置类较少见 |

---

## 五、故障注入与指标收集对应关系

| 故障类型 | 重点收集指标类别 |
|---------|-----------------|
| wordcount | hadoop, cpu, memory |
| data_skew | hadoop, jvm, cpu |
| data_bloat | hadoop, disk, network |
| task_fail | hadoop, jvm |
| long_tail | hadoop, cpu, jvm |
| wait_time | hadoop, cpu, jvm |
| runtime_delta | hadoop, jvm, cpu |
| exit_time | hadoop, jvm |
| network_latency | network, hadoop |

---

## 六、故障注入最佳实践

1. **控制变量**: 每次只注入一种故障，便于分析因果关系
2. **基线对比**: 记录无故障时的正常指标作为基线
3. **多次重复**: 每种故障多次注入，取平均值减少随机性
4. **随机顺序**: 使用随机故障顺序，避免时间相关性影响
5. **比例控制**: 设置故障/正常比例（如70%故障:30%正常）
6. **数据收集**: 故障注入前后收集足够时长的日志和指标
7. **参数调优**: 根据数据规模调整故障参数

---

## 七、故障注入命令速查表

```bash
# 基准任务
python3 common_mapreduce/inject_wordcount.py

# 数据分布类
python3 data_skew/inject_data_skew.py
BLOAT_FACTOR=10 python3 data_bloat/inject_data_bloat.py

# 任务执行类
TASK_FAIL_RATIO=0.5 python3 task_fail/inject_task_fail.py
LONG_TAIL_DURATION=60 python3 long_tail/inject_long_tail.py

# 调度类
python3 wait_time/inject_wait_time.py
python3 run_time/inject_runtime_delta.py

# 节点管理类
python3 exit_time/inject_exit_time.py

# 网络类
python3 network_latency/inject_network_latency.py

# 使用调度器批量执行
python3 collect_data/unified_scheduler_v2.py --mode sequential \
    --sequence "data_skew:1,task_fail:1,long_tail:1"
```
