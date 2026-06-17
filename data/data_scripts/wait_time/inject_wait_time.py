#!/usr/bin/env python3
"""
等待时间异常故障注入 - 挂起ResourceManager

在MapReduce任务运行期间挂起ResourceManager，导致任务等待调度时间异常
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
RM_HOST = "cxw-1"

SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True).strip()

def run_background(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


# Global state for cleanup
_rm_pid = None
_cleanup_done = False

def do_cleanup():
    global _cleanup_done
    if _cleanup_done:
        return
    _cleanup_done = True
    if _rm_pid:
        print(f'Restoring ResourceManager (PID: {_rm_pid})...')
        try:
            run(f'ssh {RM_HOST} "sudo kill -CONT {_rm_pid}"')
            print(f'  ResourceManager resumed')
        except Exception as e:
            print(f'  WARNING: Failed to resume RM: {e}')
            # Fallback: find and resume all stopped processes
            try:
                run(f'ssh {RM_HOST} "sudo bash -c \"ps aux | awk \$8~/T/ | grep -v grep | grep -v idle_inject | awk \"{{print \$2}}\" | xargs -r kill -CONT\""')
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
print("等待时间异常故障注入 - ResourceManager")
print("=" * 60)

print("\n▶ 启动 MapReduce 任务...")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/wait_time_output"

run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "reducer.py")

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -D mapreduce.job.name="wait_time_fault" \\
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
            if "RUNNING" in line and ("wait_time" in line.lower() or "streamjob" in line.lower())
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
        if "RUNNING" in line and ("wait_time" in line.lower() or "streamjob" in line.lower())
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

print(f"\n▶ 查找 ResourceManager JVM 进程 PID ...")

cmd = (
    f"ssh {RM_HOST} "
    "\"ps -eo pid,cmd | "
    "grep '[o]rg.apache.hadoop.yarn.server.resourcemanager.ResourceManager'\""
)

out = run(cmd)

if not out:
    print("✘ 未找到 ResourceManager JVM 进程")
    try:
        process.wait(timeout=300)
    except subprocess.TimeoutExpired:
        print("\n⚠ MapReduce任务超时(300s)，强制终止")
        process.kill()
        process.wait(timeout=5)
    sys.exit(1)

lines = out.splitlines()
if len(lines) != 1:
    print("✘ 匹配到多个 RM 进程，拒绝注入以避免误杀：")
    print(out)
    try:
        process.wait(timeout=300)
    except subprocess.TimeoutExpired:
        print("\n⚠ MapReduce任务超时(300s)，强制终止")
        process.kill()
        process.wait(timeout=5)
    sys.exit(1)

pid = lines[0].split()[0]
print(f"✔ ResourceManager JVM PID = {pid}")

try:
    run(f"ssh {RM_HOST} \"kill -0 {pid}\"")
except subprocess.CalledProcessError:
    print(f"✘ PID {pid} 已不存在，终止")
    try:
        process.wait(timeout=300)
    except subprocess.TimeoutExpired:
        print("\n⚠ MapReduce任务超时(300s)，强制终止")
        process.kill()
        process.wait(timeout=5)
    sys.exit(1)

print(f"\n▶ 注入等待时间异常，挂起 RM {FAULT_DURATION}s")

mark_fault_start("wait_time", {"target": "ResourceManager", "host": RM_HOST, "pid": pid})
mark_fault_injection("wait_time", f"{RM_HOST}:{pid}", "SIGSTOP", FAULT_DURATION)

run(f"ssh {RM_HOST} \"sudo kill -STOP {pid}\"")
print("✔ RM 已挂起")

time.sleep(FAULT_DURATION)

run(f"ssh {RM_HOST} \"sudo kill -CONT {pid}\"")
print("✔ RM 已恢复")

mark_fault_injection("wait_time", f"{RM_HOST}:{pid}", "SIGCONT", None)
mark_fault_end("wait_time", {"target": "ResourceManager", "host": RM_HOST, "pid": pid})

print("\n▶ 等待任务完成...")
try:
    process.wait(timeout=300)
except subprocess.TimeoutExpired:
    print("\n⚠ MapReduce任务超时(300s)，强制终止")
    process.kill()
    process.wait(timeout=5)

print("\n" + "=" * 60)
print("🎉 等待时间异常故障注入完成")
print("=" * 60)
