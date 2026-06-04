#!/usr/bin/env python3
import time
import random
import subprocess
import argparse
from datetime import datetime
import os
import threading
import sys
import enum

class Stage(enum.Enum):
    PRE_RUN  = "pre-run"   # 运行前
    POST_RUN = "post-run"  # 运行后（探测 RUNNING）
    DIR_RUN = "dir-run"  # 直接运行（不碰 Hadoop）

FAULTS = {
    "run_time": {
        "cmd": "./run_time/inject_run_time.py",
        "desc": "任务运行时间异常，挂起AM",
        "type": "run_time",
        "inject_stage": Stage.POST_RUN
    },
    "wait_time": {
        "cmd": "./wait_time/inject_wait_time.py",
        "desc": "任务等待时间异常，挂起RM",
        "type": "wait_time",
        "inject_stage": Stage.POST_RUN         # 先提交，再探测，再注入
    },
    # 以上二者区别在于，run_time可以看到所有任务时长都增加了，wait_time的每个任务时长不变的，但是会导致延迟启动
    "long_tail": {
        "cmd": "./long_tail/inject_long_tail.sh",
        "desc": "长尾任务，某些任务会有更长的运行时间",
        "type": "long_tail",
        "inject_stage": Stage.DIR_RUN
    },
    "task_fail": {
        "cmd": "./task_fail/inject_task_fail.sh",
        "desc": "任务失败",
        "type": "task_fail",
        "inject_stage": Stage.DIR_RUN
    },
    "data_bloat": {
        "cmd": "./data_bloat/inject_data_bloat.sh",
        "desc": "数据膨胀",
        "type": "data_bloat",
        "inject_stage": Stage.DIR_RUN
    },
    "data_skew": {
        "cmd": "./data_skew/inject_data_skew.sh",
        "desc": "数据倾斜",
        "type": "data_skew",
        "inject_stage": Stage.DIR_RUN
    },
}

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "scheduler.log")

def log(msg):
    os.makedirs(LOG_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def run_workload():
    cmd = "/opt/HiBench/bin/workloads/micro/wordcount/hadoop/run.sh"
    try:
        subprocess.run(cmd, shell=True, check=True)
        log("Hibench workload finished")
    except subprocess.CalledProcessError as e:
        log(f"Hibench workload failed: {e}")

def run_fault(name: str):
    fault = FAULTS[name]
    cmd_path = fault["cmd"]                # 可能是 ./xx.py 或 ./xx.sh
    log(f"▶ 注入故障: {name} | {fault.get('desc', '')}")

    # 1. 构造真正要执行的列表
    if cmd_path.endswith('.py'):
        exec_list = [sys.executable or 'python3', cmd_path]
    else:
        # .sh / 无后缀 / 自带 shebang 的可执行文件
        exec_list = [cmd_path]

    # 2. 运行
    try:
        subprocess.run(exec_list, check=True)          # 不再写死 shell=True
        log(f"✔ 故障 {name} 执行完成")
    except subprocess.CalledProcessError as e:
        log(f"✘ 故障 {name} 执行失败: {e}")

# ---------------- 新增逻辑 ----------------
def run_workload_bg():
    """后台启动 Hadoop"""
    log("启动 Hadoop 任务 (后台)")
    threading.Thread(target=run_workload, daemon=True).start()

def inject_after_delay(fault_name):
    """随机 30–45 s 后注入故障"""
    delay = random.uniform(35, 60)
    log(f"等待 {delay:.2f} 秒后注入 {fault_name}")
    time.sleep(delay)
    run_fault(fault_name)
# -----------------------------------------

def main():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--interval", type=int, default=300, help="两次故障注入的间隔秒数")
    parser.add_argument("--count", type=int, default=10, help="总共执行多少次故障注入")
    parser.add_argument("--mode", choices=["random", "round"], default="round", help="随机 or 轮询执行故障")
    parser.add_argument("--faults", nargs="*", default=list(FAULTS.keys()), help="指定可用故障列表")
    args = parser.parse_args()

    faults = args.faults
    log(f"启动故障调度器 | 模式={args.mode} | 次数={args.count} | 间隔={args.interval}s")
    log(f"可用故障: {faults}")

    idx = 0
    for i in range(args.count):
        delay = random.uniform(300, 600) # 两次故障注入的间隔秒数
        if args.mode == "random":
            fault_name = random.choice(faults)
        else:
            fault_name = faults[idx % len(faults)]
            idx += 1
        
        stage = FAULTS[fault_name].get("inject_stage")
        fault_type = FAULTS[fault_name].get("type")
        log(f"=== 第 {i+1}/{args.count} 次故障注入 ===")
        log(f"注入阶段: {stage} | 故障类型: {fault_type}")
        
        if stage == Stage.PRE_RUN:
            run_fault(fault_name)
            run_workload_bg()

        elif stage == Stage.POST_RUN:
            run_workload_bg()
            threading.Thread(target=inject_after_delay,
                            args=(fault_name,), daemon=True).start()
        else:
            # 默认：仅执行注入脚本
            run_fault(fault_name)

        if i < args.count - 1:
            log(f"休眠 {delay} 秒")
            time.sleep(delay)

    log("🎉 故障调度完成，正常退出")

if __name__ == "__main__":
    main()