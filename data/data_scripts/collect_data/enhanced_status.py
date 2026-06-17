#!/usr/bin/env python3
import csv, os, json, collections, sys, subprocess, time

DATA_DIR = "/project/data/data_scripts/collect_data/data"
DATA_MEDIUM_DIR = "/project/data/data_scripts/collect_data/data_medium"
LOG_DIR = "/project/data/data_scripts/logs"
SEQ_FILE = "/tmp/batch_sequence.txt"
PID_FILE = "/tmp/batch_scheduler.pid"
PID_FILE_PY = "/tmp/scheduler_python.pid"

TOTAL_TARGET = 5000
NORMAL_RATIO = 0.898

# 与 batch_scheduler.sh 一致的权重定义
FAULT_WEIGHTS = {
    'disk_error': 11, 'heartbeat_timeout': 9, 'network_latency': 9,
    'process_restart': 8, 'task_fail': 7, 'data_skew': 7, 'network_loss': 7,
    'data_bloat': 6, 'wait_time': 6, 'exit_time': 6, 'runtime_delta': 6,
    'long_tail': 6, 'log_level_change': 6, 'disk_full': 6
}

CATEGORIES = {
    "normal": "基准(normal)",
    "data_skew": "数据倾斜",
    "task_fail": "任务失败",
    "long_tail": "长尾任务",
    "network_latency": "网络延迟",
    "data_bloat": "数据膨胀",
    "wait_time": "等待时间",
    "runtime_delta": "运行时间",
    "exit_time": "退出时间",
    "log_level_change": "日志级别",
    "process_restart": "进程重启",
    "heartbeat_timeout": "心跳超时",
    "disk_error": "磁盘错误",
    "network_loss": "网络丢包",
    "disk_full": "磁盘满",
}

def compute_original_targets():
    """根据与 batch_scheduler.sh 一致的逻辑计算原始目标"""
    normal_target = int(TOTAL_TARGET * NORMAL_RATIO)
    fault_total = TOTAL_TARGET - normal_target
    total_weight = sum(FAULT_WEIGHTS.values())
    min_per_type = 30

    targets = collections.Counter()
    targets["normal"] = normal_target

    # 先分配最低保证
    allocated = 0
    for ft in FAULT_WEIGHTS:
        targets[ft] = min_per_type
        allocated += min_per_type

    # 按权重分配剩余
    remainder = fault_total - allocated
    if remainder > 0:
        extra_allocated = 0
        for ft, w in sorted(FAULT_WEIGHTS.items(), key=lambda x: -x[1]):
            extra = remainder * w // total_weight
            targets[ft] += extra
            extra_allocated += extra

        # 分配最终余数
        final_remainder = fault_total - allocated - extra_allocated
        if final_remainder > 0:
            for ft, w in sorted(FAULT_WEIGHTS.items(), key=lambda x: -x[1]):
                if final_remainder <= 0:
                    break
                targets[ft] += 1
                final_remainder -= 1

    return targets

def read_medium_completed():
    """扫描 data_medium/ 目录下所有 fault_labels.csv，统计已采集量"""
    done = collections.Counter()
    total_done = 0
    if not os.path.isdir(DATA_MEDIUM_DIR):
        return done, total_done
    for batch in sorted(os.listdir(DATA_MEDIUM_DIR)):
        bp = os.path.join(DATA_MEDIUM_DIR, batch)
        if not os.path.isdir(bp):
            continue
        csv_path = os.path.join(bp, "fault_labels.csv")
        if not os.path.exists(csv_path):
            continue
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                ft = row.get("fault_type", "").strip()
                if ft and ft != "fault_type":
                    done[ft] += 1
                    total_done += 1
    # wordcount 归入 normal
    if "wordcount" in done:
        done["normal"] = done.get("normal", 0) + done["wordcount"]
        del done["wordcount"]
    return done, total_done

def read_targets():
    """读取当前调度序列中的目标（仅用于显示调度信息，不再用于进度计算）"""
    targets = collections.Counter()
    if os.path.exists(SEQ_FILE):
        with open(SEQ_FILE) as f:
            content = f.read().strip()
        if ',' in content and '\n' not in content:
            items = content.split(',')
        else:
            items = content.replace(',', '\n').split('\n')
        for item in items:
            item = item.strip()
            if ':' in item:
                parts = item.split(':')
                ft = parts[0].strip()
                cnt = int(parts[1].strip())
                targets[ft] += cnt
    return targets

def _task_has_data(task_dir):
    """Check if a task directory has actual logs/metrics data"""
    if not task_dir or not os.path.isdir(task_dir):
        return False
    logs_dir = os.path.join(task_dir, 'logs')
    metrics_dir = os.path.join(task_dir, 'metrics')
    has_logs = os.path.isdir(logs_dir) and any(os.scandir(logs_dir))
    has_metrics = os.path.isdir(metrics_dir) and any(os.scandir(metrics_dir))
    return has_logs or has_metrics

def read_completed():
    """扫描 data/ 目录（续采数据）"""
    done = collections.Counter()
    total_done = 0
    total_success = 0
    total_empty = 0
    for batch in sorted(os.listdir(DATA_DIR)):
        bp = os.path.join(DATA_DIR, batch)
        if not os.path.isdir(bp):
            continue
        csv_path = os.path.join(bp, "execution_records.csv")
        if not os.path.exists(csv_path):
            continue
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                ft = row.get("fault_type", "unknown").strip('"')
                task_dir = row.get("task_dir", "")
                success = row.get("success", "").strip('"') == "True"

                # Only count tasks that have actual data
                if _task_has_data(task_dir):
                    done[ft] += 1
                    total_done += 1
                    total_success += 1
                else:
                    total_empty += 1
    if "wordcount" in done:
        done["normal"] = done.get("normal", 0) + done["wordcount"]
        del done["wordcount"]
    return done, total_done, total_success, total_empty

def _check_pid(pid):
    try:
        r = subprocess.run(
            ["ps", "-p", str(pid), "-o", "etime=", "-o", "%cpu=", "-o", "%mem="],
            capture_output=True, text=True, timeout=5
        )
        if r.returncode == 0:
            parts = r.stdout.strip().split()
            if len(parts) >= 3:
                return {
                    "pid": str(pid),
                    "etime": parts[0],
                    "cpu": parts[1],
                    "mem": parts[2],
                    "running": True
                }
    except:
        pass
    return None

def get_process_info():
    if os.path.exists(PID_FILE_PY):
        try:
            with open(PID_FILE_PY) as f:
                pid = f.read().strip()
            if pid and pid.isdigit():
                info = _check_pid(pid)
                if info:
                    return info
        except:
            pass

    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE) as f:
                pid = f.read().strip()
            if pid and pid.isdigit():
                info = _check_pid(pid)
                if info:
                    child = subprocess.run(
                        ["pgrep", "-P", pid],
                        capture_output=True, text=True, timeout=5
                    )
                    for cline in child.stdout.strip().split('\n'):
                        cpid = cline.strip()
                        if cpid and cpid.isdigit():
                            cinfo = _check_pid(cpid)
                            if cinfo:
                                return cinfo
                    return info
        except:
            pass

    try:
        result = subprocess.run(
            ["pgrep", "-f", "unified_scheduler_v2.py"],
            capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.strip().split('\n'):
            pid = line.strip()
            if pid and pid.isdigit():
                info = _check_pid(pid)
                if info:
                    return info
    except:
        pass

    try:
        result = subprocess.run(
            ["pgrep", "-f", "run_scheduler.py"],
            capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.strip().split('\n'):
            pid = line.strip()
            if pid and pid.isdigit():
                info = _check_pid(pid)
                if info:
                    child = subprocess.run(
                        ["pgrep", "-P", pid],
                        capture_output=True, text=True, timeout=5
                    )
                    for cline in child.stdout.strip().split('\n'):
                        cpid = cline.strip()
                        if cpid and cpid.isdigit():
                            cinfo = _check_pid(cpid)
                            if cinfo:
                                return cinfo
                    return info
    except:
        pass

    return None

def get_memory_info():
    try:
        with open("/proc/meminfo") as f:
            info = {}
            for line in f:
                parts = line.split()
                if parts[0] == "MemAvailable:":
                    info["available_mb"] = int(parts[1]) // 1024
                elif parts[0] == "MemTotal:":
                    info["total_mb"] = int(parts[1]) // 1024
            info["usage_pct"] = round((1 - info["available_mb"] / info["total_mb"]) * 100) if info.get("total_mb") else 0
            return info
    except:
        return {}

def get_disk_info():
    try:
        result = subprocess.run(["df", "-h", "/project"], capture_output=True, text=True, timeout=5)
        lines = result.stdout.strip().split("\n")
        if len(lines) >= 2:
            parts = lines[1].split()
            return {"avail": parts[3], "used_pct": parts[4]}
    except:
        pass
    return {}

def get_dir_size(path):
    try:
        result = subprocess.run(["du", "-sb", path], capture_output=True, text=True, timeout=60)
        if result.returncode == 0 and result.stdout.strip():
            bytes_val = int(result.stdout.split()[0])
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if bytes_val < 1024:
                    return f"{bytes_val:.1f}{unit}"
                bytes_val /= 1024
            return f"{bytes_val:.1f}PB"
    except subprocess.TimeoutExpired:
        pass
    except:
        pass
    try:
        total = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                try:
                    total += os.path.getsize(fp)
                except:
                    pass
        if total > 0:
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if total < 1024:
                    return f"{total:.1f}{unit}"
                total /= 1024
            return f"{total:.1f}PB"
    except:
        pass
    return "?"

def get_latest_log_lines(n=10):
    try:
        logs = sorted([f for f in os.listdir(LOG_DIR) if f.startswith("scheduler_") and f.endswith(".log") and f != "scheduler_restart.log"], key=lambda f: os.path.getmtime(os.path.join(LOG_DIR, f)), reverse=True)
        if not logs:
            logs = sorted([f for f in os.listdir(LOG_DIR) if f.startswith("batch_scheduler_")], key=lambda f: os.path.getmtime(os.path.join(LOG_DIR, f)), reverse=True)
        if logs:
            log_path = os.path.join(LOG_DIR, logs[0])
            with open(log_path) as f:
                lines = f.readlines()
            return logs[0], [l.rstrip() for l in lines[-n:]]
    except:
        pass
    return None, []

def main():
    # 使用原始权重计算目标
    targets = compute_original_targets()
    total_target = TOTAL_TARGET

    # 只读取 data_medium/ 目录的数据，不再统计 data/ 目录
    done, total_done = read_medium_completed()
    total_success = total_done
    total_empty = 0

    proc = get_process_info()
    mem = get_memory_info()
    disk = get_disk_info()

    W = 79

    print("")
    print("┌" + "─" * (W - 2) + "┐")
    print("│" + "  内存状态".rjust(20) + " " * (W - 2 - 20) + "│")
    print("├" + "─" * (W - 2) + "┤")
    avail = mem.get("available_mb", "?")
    usage = mem.get("usage_pct", "?")
    status = "✓ 正常" if isinstance(avail, int) and avail > 500 else "⚠ 不足"
    print(f"│ 可用内存        : {avail} MB {status}")
    print(f"│ 内存使用率      : {usage}%")
    print("└" + "─" * (W - 2) + "┘")

    print("")
    print("┌" + "─" * (W - 2) + "┐")
    print("│" + "  进程状态".rjust(20) + " " * (W - 2 - 20) + "│")
    print("├" + "─" * (W - 2) + "┤")
    if proc and proc.get("running"):
        print("│ 状态            : ✓ 运行中")
        print(f"│ PID             : {proc['pid']}")
        print(f"│ 运行时间        : {proc['etime']}")
        print(f"│ CPU使用率       : {proc['cpu']}%")
        print(f"│ 内存使用率      : {proc['mem']}%")
    else:
        print("│ 状态            : ○ 未运行")
    print("└" + "─" * (W - 2) + "┘")

    pct = (total_done * 100 // total_target) if total_target > 0 else 0
    bar_w = 40
    filled = min(total_done * bar_w // total_target, bar_w) if total_target > 0 else 0
    bar = "█" * filled + "░" * (bar_w - filled)

    normal_done = done.get("normal", 0)
    normal_tgt = targets.get("normal", 0)
    fault_done = total_done - normal_done
    fault_tgt = total_target - normal_tgt

    print("")
    print("┌" + "─" * (W - 2) + "┐")
    print("│" + "  📊 总体采集进度".rjust(25) + " " * (W - 2 - 25) + "│")
    print("├" + "─" * (W - 2) + "┤")
    print(f"│ [{bar}] {pct:3d}%")
    print(f"│ 已完成/总计     : {total_done} / {total_target}")
    print(f"│ 成功/空任务     : {total_success} / {total_empty}")
    print(f"│ 正常样本        : {normal_done} / {normal_tgt}")
    print(f"│ 故障样本        : {fault_done} / {fault_tgt}")
    print(f"│ 间隔分布        : Weibull(k=1.5, λ=180) 截断[60,400]")

    if proc and proc.get("running") and total_done > 1:
        try:
            elapsed_result = subprocess.run(
                ["ps", "-p", proc["pid"], "-o", "etimes="],
                capture_output=True, text=True, timeout=5
            )
            if elapsed_result.returncode == 0:
                elapsed = int(elapsed_result.stdout.strip())
                current_batch = sorted([d for d in os.listdir(DATA_MEDIUM_DIR) if d.startswith("batch_")], reverse=True)
                tasks_this_run = 0
                if current_batch:
                    batch_csv = os.path.join(DATA_MEDIUM_DIR, current_batch[0], "fault_labels.csv")
                    if os.path.exists(batch_csv):
                        with open(batch_csv) as _f:
                            tasks_this_run = sum(1 for _ in csv.DictReader(_f))
                remaining_tasks = total_target - total_done
                secs_per_task = 600
                if tasks_this_run > 1:
                    secs_per_task = max(600, min(elapsed / tasks_this_run, 900))
                remain_secs = remaining_tasks * secs_per_task
                remain_h = int(remain_secs // 3600)
                remain_d = remain_h // 24
                remain_h = remain_h % 24
                remain_m = int((remain_secs % 3600) // 60)
                if remain_d > 0:
                    print(f"│ 预计剩余时间    : ~{remain_d}d {remain_h}h {remain_m}m")
                else:
                    print(f"│ 预计剩余时间    : ~{remain_h}h {remain_m}m")
        except:
            pass
    print("└" + "─" * (W - 2) + "┘")

    print("")
    print("┌" + "─" * (W - 2) + "┐")
    print("│" + "  📋 各故障类别采集进度".rjust(30) + " " * (W - 2 - 30) + "│")
    print("├" + "─" * (W - 2) + "┤")
    print(f"│ {'故障类型':<14} {'完成/目标':>10} {'完成率':>7}  {'进度条':<20} │")

    order = ["normal"]
    others = sorted([k for k in targets if k != "normal"], key=lambda x: targets[x], reverse=True)
    order.extend(others)

    for ft in order:
        tgt = targets.get(ft, 0)
        dn = done.get(ft, 0)
        p = (dn * 100 // tgt) if tgt > 0 else (100 if dn > 0 else 0)
        bw = 20
        bf = min(dn * bw // tgt, bw) if tgt > 0 else bw
        b = "█" * bf + "░" * (bw - bf)
        name = CATEGORIES.get(ft, ft)
        print(f"│ {name:<14} {dn:>3}/{tgt:<6} {p:>5}%  {b} │")

    print("├" + "─" * (W - 2) + "┤")
    tp = (total_done * 100 // total_target) if total_target > 0 else 0
    print(f"│ {'合计':<14} {total_done:>3}/{total_target:<6} {tp:>5}%")
    print("└" + "─" * (W - 2) + "┘")

    print("")
    print("┌" + "─" * (W - 2) + "┐")
    print("│" + "  磁盘使用情况".rjust(20) + " " * (W - 2 - 20) + "│")
    print("├" + "─" * (W - 2) + "┤")
    print(f"│ 旧数据(data/)   : {get_dir_size(DATA_DIR)}")
    print(f"│ 数据目录大小    : {get_dir_size(DATA_MEDIUM_DIR)}")
    print(f"│ 日志目录大小    : {get_dir_size(LOG_DIR)}")
    print(f"│ 磁盘可用空间    : {disk.get('avail', '?')} ({disk.get('used_pct', '?')} 已用)")
    print("└" + "─" * (W - 2) + "┘")

    log_name, log_lines = get_latest_log_lines(10)
    print("")
    print("┌" + "─" * (W - 2) + "┐")
    print("│" + "  最近日志 (最后10行)".rjust(30) + " " * (W - 2 - 30) + "│")
    print("├" + "─" * (W - 2) + "┤")
    if log_name:
        print(f"│ 日志文件: {log_name}")
        print("├" + "─" * (W - 2) + "┤")
        for line in log_lines:
            print(f"│ {line[:75]}")
    else:
        print("│ 暂无日志文件")
    print("└" + "─" * (W - 2) + "┘")

if __name__ == "__main__":
    main()
