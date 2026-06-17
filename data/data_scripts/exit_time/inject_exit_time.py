#!/usr/bin/env python3
"""
退出时间异常故障注入 - 挂起NodeManager

在MapReduce任务运行期间挂起NodeManager进程，导致任务退出时间异常，任务完成时间变长
"""
import subprocess
import time
import sys
sys.stdout.reconfigure(line_buffering=True)
import os
import signal
import atexit

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection


os.environ["PATH"] = "/opt/hadoop/bin:" + os.environ.get("PATH", "")
FAULT_DURATION = 60
WORKER_NODES = ["cxw-2", "cxw-3", "cxw-4"]

SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True).strip()

def run_background(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


# Global state for cleanup
_suspended_nodes = []
_cleanup_done = False

def do_cleanup():
    global _cleanup_done
    if _cleanup_done:
        return
    _cleanup_done = True
    for node, pid in _suspended_nodes:
        try:
            run(f'ssh {node} "sudo kill -CONT {pid}"')
            print(f'  Resumed NodeManager on {node} (PID: {pid})')
        except Exception as e:
            print(f'  WARNING: Failed to resume NM on {node}: {e}')
    # Fallback: resume all stopped processes on all worker nodes
    for node in WORKER_NODES:
        try:
            run(f'ssh {node} "sudo bash -c \"ps aux | awk \$8~/T/ | grep -v grep | grep -v idle_inject | awk \"{{print \$2}}\" | xargs -r kill -CONT\""')
        except:
            pass

def signal_handler(signum, frame):
    print(f'\nWARNING: Signal received, cleaning up...')
    do_cleanup()
    sys.exit(130)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
atexit.register(do_cleanup)

print("=" * 60)
print("退出时间异常故障注入 - NodeManager")
print("=" * 60)

print("\n▶ 启动 MapReduce 任务...")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/exit_time_output"

run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "reducer.py")

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -D mapreduce.job.name="exit_time_fault" \\
    -D mapreduce.job.maps=24 \\
    -D mapreduce.job.reduces=8 \\
    -inputformat org.apache.hadoop.mapred.SequenceFileInputFormat \\
    -input {input_path} \\
    -output {output_path} \\
    -mapper "python3 mapper.py" \\
    -reducer "python3 reducer.py" \\
    -file {mapper_path} \\
    -file {reducer_path}
"""

process = run_background(cmd)

print("  任务已启动，等待任务初始化...")
time.sleep(5)

print("\n▶ 查找正在运行的 MapReduce 任务...")

found = False
app_id = None
max_retries = 30

for i in range(max_retries):
    try:
        list_output = run("yarn application -list 2>/dev/null || true")
        running_lines = [
            line for line in list_output.splitlines()
            if "RUNNING" in line and ("exit_time" in line.lower() or "streamjob" in line.lower())
        ]

        if running_lines:
            app_id = running_lines[0].split()[0]
            print(f"  ✔ 找到任务: {app_id}")
            found = True
            break
    except Exception as e:
        print(f"  查找任务出错: {e}")
        pass

    time.sleep(2)

if not found:
    list_output = run("yarn application -list 2>/dev/null || true")
    running_lines = [
        line for line in list_output.splitlines()
        if "RUNNING" in line and ("exit_time" in line.lower() or "streamjob" in line.lower())
    ]
    if running_lines:
        app_id = running_lines[0].split()[0]
        print(f"  ✔ 找到任务: {app_id}")
        found = True

if not found:
    print("  ✘ 未找到运行中的任务，跳过故障注入")
    try:
        process.wait(timeout=300)
    except subprocess.TimeoutExpired:
        print("\n⚠ MapReduce任务超时(300s)，强制终止")
        process.kill()
        process.wait(timeout=5)
    sys.exit(0)

print(f"\n▶ 在所有工作节点上查找 NodeManager PID...")

suspended_nodes = []
for node in WORKER_NODES:
    try:
        pid = run(
            f"ssh {node} \"ps -eo pid,cmd | grep '[o]rg.apache.hadoop.yarn.server.nodemanager.NodeManager' | head -n 1 | awk '{{print \\$1}}'\""
        ).strip()

        if pid:
            print(f"  {node}: NodeManager PID = {pid}")
            suspended_nodes.append((node, pid)); _suspended_nodes.append((node, pid))
        else:
            print(f"  ✘ {node}: 未找到 NodeManager 进程")
    except Exception as e:
        print(f"  ✘ {node}: 查找PID失败 - {e}")

if not suspended_nodes:
    print("\n✘ 未找到任何NodeManager进程，跳过故障注入")
    try:
        process.wait(timeout=300)
    except subprocess.TimeoutExpired:
        print("\n⚠ MapReduce任务超时(300s)，强制终止")
        process.kill()
        process.wait(timeout=5)
    sys.exit(1)

print(f"\n▶ 注入退出时间异常，挂起 NodeManager {FAULT_DURATION} 秒...")

mark_fault_start("exit_time", {"target": "NodeManager", "nodes": [n for n, _ in suspended_nodes]})

for node, pid in suspended_nodes:
    try:
        run(f"ssh {node} \"kill -0 {pid}\"")
        run(f"ssh {node} \"sudo kill -STOP {pid}\"")
        print(f"  ✔ 已挂起 {node} 上的 NodeManager (PID: {pid})")
        mark_fault_injection("exit_time", f"{node}:{pid}", "SIGSTOP", FAULT_DURATION)
    except subprocess.CalledProcessError:
        print(f"  ✘ {node}: PID {pid} 已不存在，跳过")
    except Exception as e:
        print(f"  ✘ {node}: 挂起失败 - {e}")

print(f"\n⏳ 等待 {FAULT_DURATION} 秒...")
for i in range(FAULT_DURATION):
    time.sleep(1)
    if i % 30 == 0 and i > 0:
        print(f"  已等待 {i} 秒...")

print("\n▶ 恢复 NodeManager...")

for node, pid in suspended_nodes:
    try:
        run(f"ssh {node} \"sudo kill -CONT {pid}\"")
        print(f"  ✔ 已恢复 {node} 上的 NodeManager (PID: {pid})")
        mark_fault_injection("exit_time", f"{node}:{pid}", "SIGCONT", None)
    except Exception as e:
        print(f"  ✘ 恢复 {node} 失败: {e}")

_cleanup_done = True; mark_fault_end("exit_time", {"target": "NodeManager", "nodes": [n for n, _ in suspended_nodes]})

print("\n▶ 等待任务完成...")
try:
    process.wait(timeout=300)
except subprocess.TimeoutExpired:
    print("\n⚠ MapReduce任务超时(300s)，强制终止")
    process.kill()
    process.wait(timeout=5)

print("\n" + "=" * 60)
print("🎉 退出时间异常故障注入完成")
print("=" * 60)
