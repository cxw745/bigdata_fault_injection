#!/usr/bin/env python3
"""
运行时间异常故障注入 (v3 - 健壮恢复版)

修复说明：
- v3修复：
  1. 恢复时用 sudo kill -CONT {pid} 2>/dev/null || true 忽略错误
  2. 增加signal handler确保Ctrl+C时恢复进程
  3. 增加恢复验证步骤
"""
import subprocess
import time
import sys
sys.stdout.reconfigure(line_buffering=True)
import re
import os
import signal
import atexit

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection

os.environ["PATH"] = "/opt/hadoop/bin:" + os.environ.get("PATH", "")

SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FAULT_DURATION = int(os.environ.get("FAULT_DURATION", "120"))
INJECT_DELAY = int(os.environ.get("INJECT_DELAY", "15"))

# 全局状态
_all_stopped = []
_cleanup_done = False

def run(cmd):
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()
    except subprocess.CalledProcessError as e:
        return e.output.strip() if e.output else ""

def run_bg(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def do_cleanup():
    """执行完整的恢复清理逻辑"""
    global _cleanup_done
    if _cleanup_done:
        return
    _cleanup_done = True

    print("\n▶ 执行恢复清理...")
    for node, stopped_pid, ptype in _all_stopped:
        try:
            run(f'ssh {node} "echo ubuntu | sudo -S kill -CONT {stopped_pid} 2>/dev/null || true"')
            print(f"  ✔ 已恢复 {ptype} (PID: {stopped_pid} on {node})")
        except:
            pass

    # 验证恢复
    verify_recovery()

def verify_recovery():
    """验证所有被STOP的进程已恢复"""
    print("\n▶ 验证恢复结果...")
    all_resumed = True
    for node, stopped_pid, ptype in _all_stopped:
        try:
            # 检查进程是否还在且状态不是T (stopped)
            result = run(f'ssh {node} "ps -p {stopped_pid} -o stat= 2>/dev/null || echo GONE"')
            if "GONE" in result:
                print(f"  ✔ {ptype} (PID: {stopped_pid} on {node}): 进程已结束（正常，可能被YARN回收）")
            elif "T" in result:
                print(f"  ✘ {ptype} (PID: {stopped_pid} on {node}): 进程仍处于STOP状态")
                all_resumed = False
            else:
                print(f"  ✔ {ptype} (PID: {stopped_pid} on {node}): 进程已恢复运行")
        except:
            print(f"  ⚠ {ptype} (PID: {stopped_pid} on {node}): 无法验证状态")

    if all_resumed:
        print("  ✔ 所有进程恢复验证通过")
    else:
        print("  ⚠ 部分进程可能仍处于STOP状态，请手动检查")

def signal_handler(signum, frame):
    """处理中断信号"""
    sig_name = signal.Signals(signum).name
    print(f"\n⚠ 收到信号 {sig_name}，执行清理...")
    do_cleanup()
    sys.exit(130)

# 注册signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
atexit.register(do_cleanup)

print("=" * 60)
print("运行时间异常故障注入 (v3 - 健壮恢复版)")
print("=" * 60)

print(f"\n▶ 故障参数:")
print(f"  故障持续时间: {FAULT_DURATION}s")
print(f"  注入延迟: {INJECT_DELAY}s")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/runtime_delta_output"

run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "reducer.py")

mark_fault_start("runtime_delta", {"duration": FAULT_DURATION, "inject_delay": INJECT_DELAY})

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -D mapreduce.job.name="runtime_delta_fault" \\
    -D mapreduce.job.maps=24 \\
    -D mapreduce.job.reduces=8 \\
    -D yarn.am.liveness-monitor.expiry-interval-ms=600000 \\
    -inputformat org.apache.hadoop.mapred.SequenceFileInputFormat \\
    -input {input_path} \\
    -output {output_path} \\
    -mapper "python3 mapper.py" \\
    -reducer "python3 reducer.py" \\
    -file {mapper_path} \\
    -file {reducer_path}
"""

process = run_bg(cmd)

print(f"\n▶ 等待任务充分运行 ({INJECT_DELAY}s)...")
time.sleep(INJECT_DELAY)

print("\n▶ 查找运行中的任务...")
app_id = None
for i in range(30):
    try:
        list_output = run("yarn application -list 2>/dev/null || true")
        running_lines = [
            line for line in list_output.splitlines()
            if "RUNNING" in line and ("runtime_delta" in line.lower() or "stream job" in line.lower())
        ]
        if running_lines:
            app_id = running_lines[0].split()[0]
            print(f"  ✔ 找到任务: {app_id}")
            break
    except:
        pass
    time.sleep(2)

if not app_id:
    print("  ✘ 未找到运行中的任务，跳过故障注入")
    try:
        process.wait(timeout=300)
    except subprocess.TimeoutExpired:
        process.kill()
    sys.exit(0)

# 获取AM Host
print(f"\n▶ 查询 ApplicationMaster 所在节点...")
status = run(f"yarn application -status {app_id}")
match = re.search(r"AM Host\s*:\s*([a-zA-Z0-9\-]+)", status)
if not match:
    print("  ✘ 未找到 AM Host 信息")
    try:
        process.wait(timeout=300)
    except subprocess.TimeoutExpired:
        process.kill()
    sys.exit(1)

am_host = match.group(1)
print(f"  ✔ AM 运行节点: {am_host}")

# 查找MRAppMaster PID
print(f"\n▶ 在 {am_host} 上查找 MRAppMaster PID...")
pid = run(
    f"ssh {am_host} \"ps -eo pid,ppid,cmd --sort=-rss | grep MRAppMaster | grep -v grep | head -n 1 | awk '{{print \\$1}}'\""
).strip()

if not pid:
    print(f"  ✘ 在 {am_host} 上找不到 MRAppMaster 主进程")
    try:
        process.wait(timeout=300)
    except subprocess.TimeoutExpired:
        process.kill()
    sys.exit(1)

print(f"  ✔ MRAppMaster PID: {pid}")

# 同时查找YarnChild进程(部分Container)
print(f"\n▶ 查找 Container 进程...")
container_pids = []
try:
    result = run(f"ssh {am_host} \"ps -eo pid,cmd | grep YarnChild | grep -v grep | head -n 3 | awk '{{print \\$1}}'\"")
    container_pids = [p.strip() for p in result.split('\n') if p.strip()]
    print(f"  找到 {len(container_pids)} 个 Container 进程")
except:
    print("  未找到 Container 进程")

# 在其他节点也查找Container
for node in ["cxw-2", "cxw-3", "cxw-4"]:
    if node == am_host:
        continue
    try:
        result = run(f"ssh {node} \"ps -eo pid,cmd | grep YarnChild | grep -v grep | head -n 2 | awk '{{print \\$1}}'\"")
        node_pids = [p.strip() for p in result.split('\n') if p.strip()]
        for p in node_pids:
            container_pids.append(f"{node}:{p}")
        if node_pids:
            print(f"  {node}: 找到 {len(node_pids)} 个 Container 进程")
    except:
        pass

# 注入故障
print(f"\n▶ 注入运行时间异常，挂起 AM {FAULT_DURATION}s...")

# 1. 挂起 MRAppMaster
mark_fault_injection("runtime_delta", f"{am_host}:{pid}", "SIGSTOP_AM", FAULT_DURATION)
run(f'ssh {am_host} "echo ubuntu | sudo -S kill -STOP {pid}"')
_all_stopped.append((am_host, pid, "AM"))
print(f"  ✔ 已挂起 MRAppMaster (PID: {pid})")

# 2. 挂起部分Container（最多3个，避免全部挂起导致任务直接失败）
stopped_containers = 0
for cp in container_pids[:3]:
    if ':' in cp:
        node, cpid = cp.split(':')
    else:
        node, cpid = am_host, cp
    try:
        run(f'ssh {node} "echo ubuntu | sudo -S kill -STOP {cpid}"')
        _all_stopped.append((node, cpid, "Container"))
        stopped_containers += 1
        print(f"  ✔ 已挂起 Container (PID: {cpid} on {node})")
    except:
        pass

print(f"  共挂起: 1 AM + {stopped_containers} Containers")

try:
    time.sleep(FAULT_DURATION)
except KeyboardInterrupt:
    print("\n  收到中断信号，提前终止...")

# 恢复
print(f"\n▶ 恢复所有进程...")
for node, stopped_pid, ptype in _all_stopped:
    try:
        run(f'ssh {node} "echo ubuntu | sudo -S kill -CONT {stopped_pid} 2>/dev/null || true"')
        print(f"  ✔ 已恢复 {ptype} (PID: {stopped_pid} on {node})")
    except:
        print(f"  ⚠ 恢复 {ptype} (PID: {stopped_pid} on {node}) 失败（进程可能已结束）")

# 标记清理完成，防止atexit重复执行
_cleanup_done = True

# 验证恢复
verify_recovery()

mark_fault_injection("runtime_delta", f"{am_host}:{pid}", "SIGCONT", None)
mark_fault_end("runtime_delta", {
    "target": "MRAppMaster+Containers",
    "host": am_host,
    "pid": pid,
    "stopped_containers": stopped_containers,
    "duration": FAULT_DURATION,
})

print("\n▶ 等待任务完成...")
try:
    process.wait(timeout=300)
except subprocess.TimeoutExpired:
    print("\n⚠ MapReduce任务超时(300s)，强制终止")
    process.kill()
    process.wait(timeout=5)

print("\n" + "=" * 60)
print("运行时间异常故障注入完成 (v3)")
print("=" * 60)
