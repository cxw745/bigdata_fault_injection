#!/usr/bin/env python3
"""
长尾任务故障注入 - Mapper

支持多种注入模式：
1. task_id: 基于任务ID确定性注入（推荐）
2. ratio: 基于比例精确控制
3. probability: 基于概率（旧方式，不推荐）
"""
import sys
import os
import time
import random

INJECT_MODE = os.environ.get("LONG_TAIL_MODE", "task_id")
INJECT_TASK_IDS = os.environ.get("LONG_TAIL_TASK_IDS", "0,5,10")
INJECT_RATIO = float(os.environ.get("LONG_TAIL_RATIO", "0.3"))
INJECT_PROBABILITY = float(os.environ.get("LONG_TAIL_PROBABILITY", "0.3"))
INJECT_DURATION = int(os.environ.get("LONG_TAIL_DURATION", "120"))

def get_task_id():
    """
    获取当前Map任务ID
    MapReduce会设置环境变量 mapreduce_task_id (如: task_123456789_0001_m_000000)
    """
    task_id_str = os.environ.get("mapreduce_task_id", "")
    if task_id_str:
        parts = task_id_str.split("_")
        if len(parts) >= 6:
            try:
                return int(parts[-1])
            except ValueError:
                pass
    
    task_attempt_id = os.environ.get("mapreduce_task_attempt_id", "")
    if task_attempt_id:
        parts = task_attempt_id.split("_")
        if len(parts) >= 6:
            try:
                return int(parts[-1])
            except ValueError:
                pass
    
    return None

def should_inject_by_task_id():
    """基于任务ID确定性注入"""
    task_id = get_task_id()
    if task_id is None:
        return random.random() < INJECT_PROBABILITY
    
    target_ids = [int(x.strip()) for x in INJECT_TASK_IDS.split(",") if x.strip().isdigit()]
    return task_id in target_ids

def should_inject_by_ratio():
    """基于比例精确控制"""
    task_id = get_task_id()
    if task_id is None:
        return random.random() < INJECT_RATIO
    
    total_maps = int(os.environ.get("mapreduce_job_maps", "24"))
    inject_count = max(1, int(total_maps * INJECT_RATIO))
    
    target_ids = list(range(inject_count))
    return task_id in target_ids

def should_inject_by_probability():
    """基于概率注入（旧方式）"""
    return random.random() < INJECT_PROBABILITY

def inject_long_tail():
    """注入长尾延迟"""
    should_inject = False
    
    if INJECT_MODE == "task_id":
        should_inject = should_inject_by_task_id()
    elif INJECT_MODE == "ratio":
        should_inject = should_inject_by_ratio()
    else:
        should_inject = should_inject_by_probability()
    
    if should_inject:
        task_id = get_task_id()
        sys.stderr.write(f"[LONG_TAIL] Injecting {INJECT_DURATION}s delay for task_id={task_id}\n")
        sys.stderr.flush()
        time.sleep(INJECT_DURATION)

inject_long_tail()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    for word in line.split():
        print(f"{word}\t1", flush=True)
