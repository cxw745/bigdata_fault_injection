# FaultLLM 项目当前状态

更新日期: 2026-05-29

---

## 项目目标

构建 Hadoop 集群故障诊断数据集，通过故障注入收集多模态数据（日志+指标+拓扑），训练 LLM 进行故障根因分析。

---

## 核心架构

```
cxw-1 (Master) ── RM/NM/NameNode/JobHistory
├── cxw-2 (Worker) ── NM/DataNode
├── cxw-3 (Worker) ── NM/DataNode
└── cxw-4 (Worker) ── NM/DataNode
```

### 数据收集链路

```
故障注入脚本 → Hadoop任务执行 → Prometheus/Loki采集 → CSV保存到batch目录
                                                         ↓
                                              RM/NM日志直接从磁盘读取
```

---

## 服务状态（全部运行中）

| 服务 | 节点 | 端口 | 状态 |
|------|------|------|------|
| NameNode | cxw-1 | 9870 | ✅ |
| ResourceManager | cxw-1 | 8088 | ✅ |
| JobHistoryServer | cxw-1 | 19888 | ✅ |
| NodeManager | cxw-2/3/4 | - | ✅ |
| Loki | cxw-1 | 3100 | ✅ |
| Grafana | cxw-1 | 3000 | ✅ |
| 故障注入调度器 | cxw-1 | - | ✅ PID 31413 |

---

## 数据收集进度

**总计: 1839 / 1632 好样本 (113%) ✅ 全部达标** — 60个空任务已排除

| 故障类型 | 已收集/需要 | 进度 | 
|---------|------------|------|
| normal | 1468/1311 | ████████████████████ 111% ✅ |
| disk_error | 37/28 | ████████████████████████ 132% ✅ |
| runtime_delta | 35/27 | ███████████████████████ 129% ✅ |
| exit_time | 33/26 | ███████████████████████ 126% ✅ |
| task_fail | 24/20 | ██████████████████████ 120% ✅ |
| log_level_change | 27/23 | █████████████████████ 117% ✅ |
| long_tail | 24/21 | █████████████████████ 114% ✅ |
| data_bloat | 26/23 | ████████████████████ 113% ✅ |
| network_latency | 26/23 | ████████████████████ 113% ✅ |
| normal | 1468/1311 | ████████████████████ 111% ✅ |
| heartbeat_timeout | 31/28 | ██████████████████ 110% ✅ |
| process_restart | 31/28 | ██████████████████ 110% ✅ |
| wait_time | 25/23 | █████████████████ 108% ✅ |
| permission_denied | 30/28 | ████████████████ 107% ✅ |
| data_skew | 22/23 | ████████████████ 95% ⚠️ 差1个 |

**数据收集已全部完成！**

---

## 已完成的修复

| 日期 | 问题 | 修复内容 |
|------|------|---------|
| 05-22 | exit_time 卡死调度器 | `_run_fault_script` 改用 `select.select` 非阻塞读取，timeout 真正生效 |
| 05-22 | application_id 不捕获 | YARN FINISHED/FAILED/KILLED 回退查询，3处注入 |
| 05-22 | Normal 任务超时无数据 | timeout 600→120s，超时后仍收集部分数据 |
| 05-22 | RM/NM 日志缺失 | `collect_rm_nm_logs_from_disk()` 直接从磁盘读取 RM/NM 日志（按时间窗口过滤），历史和未来数据均覆盖 |
| 05-22 | RM/NM 退化 | 重启 ResourceManager + 3 NodeManagers 恢复集群 |
| 05-22 | Status 显示错误 | PID 自动保存、ETA 修复、日志文件排序修复 |
| 05-22 | Interval 调整 | 60-600 → 60-400 |

---

## 数据集结构

```
/project/data/data_scripts/collect_data/data/
├── batch_YYYYMMDD_HHMMSS/
│   ├── execution_records.csv       # 执行记录（含app_id/duration/success）
│   ├── fault_labels.csv            # 故障标签（只含好样本）
│   ├── experiment_config.json      # 实验配置
│   ├── collection_statistics.json  # 批次统计（批次完成后生成）
│   └── <故障类型>_<app_id>_<时间戳>/
│       ├── fault_injection_detail.json   # 故障注入元数据
│       ├── logs/
│       │   ├── cxw-1/resourcemanager_*.csv     # RM日志（磁盘读取）✅
│       │   ├── cxw-1/namenode_*.csv             # NN日志（Loki）
│       │   ├── cxw-2/nodemanager_*.csv          # NM日志（磁盘读取）✅
│       │   └── cxw-2/datanode_*.csv             # DN日志（Loki）
│       └── metrics/
│           ├── cxw-1/hadoop_resourcemanager_*   # RM指标（Prometheus）
│           ├── cxw-1/hadoop_namenode_*          # NN指标
│           ├── cxw-2/hadoop_nodemanager_*       # NM指标
│           ├── cxw-2/hadoop_datanode_*          # DN指标
│           ├── cxw-*/node_{cpu,memory,disk,network}_*  # 系统指标
│           └── cxw-*/jvm_*                      # JVM指标
```

### 关键路径

| 内容 | 路径 |
|------|------|
| 数据根目录 | `/project/data/data_scripts/collect_data/data/` |
| 调度器日志 | `/project/data/data_scripts/logs/scheduler_*.log` |
| 调度器脚本 | `/project/data/data_scripts/collect_data/unified_scheduler_v2.py` |
| 日志收集器 | `/project/data/data_scripts/collect_data/log_collector.py` |
| 指标收集器 | `/project/data/data_scripts/collect_data/metrics_collector.py` |
| 状态查看 | `/project/data/data_scripts/check_status.py` |
| 管理脚本 | `/project/data/data_scripts/batch_scheduler.sh` |
| 故障脚本目录 | `/project/data/data_scripts/{fault_type}/inject_{fault_type}.py` |

### 已收集数据量

- 总数据: **6.5 GB**
- 批次: **15 个**（前 8 批含旧代码收集的空任务，已清洗）
- 好样本: **772 个**（有完整日志+指标+标签）
- 空任务: **60 个**（无数据，已从 fault_labels.csv 排除）

---

## 故障类型与注入方式

| 类型 | 注入方法 | 影响 | 检测信号 |
|------|---------|------|---------|
| wait_time | SIGSTOP 挂起 ResourceManager 120s | 集群级调度暂停 | RM CPU→0, 容器分配停止 |
| exit_time | SIGSTOP 挂起 NodeManager 60s | 节点级任务中断 | NM 容器数下降, 任务失败 |
| runtime_delta | SIGSTOP 挂起 MRAppMaster 120s | 任务级进度停滞 | AM 心跳停止, 任务延时 |
| data_skew | Mapper 输出分布不均 | 数据倾斜 | Reducer 执行时间差异大 |
| data_bloat | Mapper 输出数据膨胀 | 数据量异常 | Map 输出量突增 |
| task_fail | Mapper 随机抛 RuntimeException | 任务失败 | 容器失败数突增 |
| long_tail | Mapper 注入延迟 | 长尾任务 | 部分任务执行时间异常 |
| network_latency | tc 命令注入网络延迟 | 网络延迟 | TCP 重传率上升 |
| log_level_change | Hadoop logLevel Servlet 改日志级别 | 日志量暴增 | 日志产生速率变化 |
| process_restart | 进程重启 | 服务中断 | RM/NM 进程重启事件 |
| permission_denied | 修改 HDFS 文件权限 | 权限错误 | 访问拒绝日志 |
| heartbeat_timeout | 网络分区/延迟 | 心跳超时 | RM 报告 NM 丢失 |
| disk_error | 填充磁盘/模拟IO错误 | 存储故障 | 磁盘指标异常 |

---

## SSH 连接

```bash
ssh cxw-1    # Hadoop Master (10.10.0.82)
ssh cxw-2    # Worker 1
ssh cxw-3    # Worker 2
ssh cxw-4    # Worker 3
```

---

## 常用操作

### 查看状态
```bash
ssh cxw-1 "bash /project/data/data_scripts/batch_scheduler.sh status"
# 或
ssh cxw-1 "python3 /project/data/data_scripts/check_status.py"
```

### 查看实时日志
```bash
ssh cxw-1 "tail -f /project/data/data_scripts/logs/scheduler_$(ls -t /project/data/data_scripts/logs/scheduler_*.log | head -1 | xargs basename)"
```

### 查看当前批次
```bash
ssh cxw-1 "ls /project/data/data_scripts/collect_data/data/ | tail -3"
```

### 手动启动调度器
```bash
ssh cxw-1 "nohup python3 /project/data/data_scripts/collect_data/run_scheduler.py \
  --sequence-file /tmp/new_batch_sequence.txt \
  --data-size small \
  --workload wordcount \
  --interval-min 60 \
  --interval-max 400 \
  --max-batch-size 100 \
  --output-dir /project/data/data_scripts/collect_data/data \
  > /project/data/data_scripts/logs/scheduler_\$(date +%Y%m%d_%H%M%S).log 2>&1 &"
```

### 停止调度器
```bash
ssh cxw-1 "kill -9 \$(pgrep -f unified_scheduler_v2.py) 2>/dev/null; kill -9 \$(pgrep -f run_scheduler.py) 2>/dev/null"
```

---

## 已完成的实验/分析

| 文件 | 内容 |
|------|------|
| `results/all_results_summary.json` | 2 个模型训练实验结果 |
| `results/experiment_report.md` | 实验报告 |
| `results/full_experiment_report.md` | 完整实验分析报告 |
| `experiment_analysis_report.md` | 实验分析 |
| `PROJECT_SUMMARY.md` | 项目摘要 |
| `FaultLLM_进展报告_20260509.md` | 进展报告 |

---

## 待办

- [ ] 数据收集完成后，同步到 GPU 训练服务器 (`/data/dds-data/cxw/collect_data/`)
- [ ] 运行 `run_remaining_121.py` 进行模型训练实验
- [ ] 运行 `run_optimized_v2.py` 验证优化方案
- [ ] 数据质量分析报告
- [ ] ICSE 2027 论文投稿准备

---

## 已知问题

- Lok i 只有 5 个日志源（缺少 RM/NM 日志流），但 `collect_rm_nm_logs_from_disk()` 直接从磁盘读取弥补了此缺陷
- normal 模式下 Hadoop streaming 的 mapper/reducer 脚本路径使用硬编码，如果目录迁移需要更新
- 每 ~10 天需检查磁盘空间（当前 92G/117G 可用）
- 旧批次（05-18 前）使用了不同的集群（cpf-1/2/3/4），与新数据不兼容
