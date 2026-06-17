#!/usr/bin/env python3
"""显示真实的数据收集进度（只统计有实际数据的样本）"""
import csv, os, sys

data_dir = '/project/data/data_scripts/collect_data/data'

required = {'permission_denied':28,'disk_error':28,'long_tail':21,'exit_time':26,
            'wait_time':23,'log_level_change':23,'data_bloat':23,'runtime_delta':27,
            'data_skew':23,'network_latency':23,'process_restart':28,'normal':1311,
            'heartbeat_timeout':28,'task_fail':20}

def task_has_data(task_dir):
    """Check if a task directory has actual logs and metrics data"""
    logs_dir = os.path.join(task_dir, 'logs')
    metrics_dir = os.path.join(task_dir, 'metrics')
    has_logs = os.path.isdir(logs_dir) and any(os.scandir(logs_dir))
    has_metrics = os.path.isdir(metrics_dir) and any(os.scandir(metrics_dir))
    return has_logs or has_metrics

counts = {}
empty_tasks = 0
total_dirs = 0

for batch in sorted(os.listdir(data_dir)):
    batch_dir = os.path.join(data_dir, batch)
    if not os.path.isdir(batch_dir):
        continue
    ecsv = os.path.join(batch_dir, 'execution_records.csv')
    if not os.path.exists(ecsv):
        continue
    with open(ecsv) as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_dirs += 1
            task_dir = row.get('task_dir', '')
            app_id = row.get('application_id', '')
            success = row.get('success', 'False')
            dur = int(row.get('duration', '0'))

            # Check if task actually has data
            if task_dir and os.path.isdir(task_dir) and task_has_data(task_dir):
                ft = row['fault_type']
                ft_key = 'normal' if ft == 'wordcount' else ft
                counts[ft_key] = counts.get(ft_key, 0) + 1
            else:
                empty_tasks += 1

print('=' * 65)
print('  FaultLLM 数据收集真实进度')
print('=' * 65)
print()
print('  %-22s %6s / %-6s' % ('故障类型', '已收集', '需要'))
print('  ' + '-' * 40)

remaining_parts = []
total_good = 0
total_need = 0

for ft, req in sorted(required.items()):
    got = counts.get(ft, 0)
    rem = max(0, req - got)
    total_good += got
    total_need += req
    if rem > 0:
        remaining_parts.append('%s:%d' % (ft, rem))

    pct = int(got / max(req, 1) * 100)
    bar_len = int(got / max(req, 1) * 20)
    bar = '#' * bar_len + ' ' * (20 - bar_len)
    print('  %-22s %6d / %-6d [%s] %3d%%' % (ft, got, req, bar, pct))

print('  ' + '-' * 40)
print('  %-22s %6d / %-6d' % ('总计', total_good, total_need))
print()
print('  空任务(无数据): %d / %d 总目录' % (empty_tasks, total_dirs))
print()

# Current run info
print('  --- 当前运行 ---')
log_dir = '/project/data/data_scripts/logs'
logs = sorted([f for f in os.listdir(log_dir) if f.startswith('scheduler_') and f.endswith('.log')])
if logs:
    latest = logs[-1]
    with open(os.path.join(log_dir, latest)) as f:
        lines = f.readlines()
        for line in reversed(lines[-5:]):
            if '执行:' in line or '等待' in line:
                print('  ' + line.strip())
                break

print()
seq_str = ','.join(remaining_parts)
print('  剩余 sequence (%d tasks):' % (total_need - total_good))
print('  ' + seq_str[:120] + ('...' if len(seq_str) > 120 else ''))
print()
print('=' * 65)
