#!/usr/bin/env python3
import subprocess
import time
import sys
import re
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection

FAULT_DURATION = 60

SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True).strip()

def run_background(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

print("=" * 60)
print("运行时间异常故障注入")
print("=" * 60)

print("\n▶ 启动 MapReduce 任务...")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/runtime_delta_output"

run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "reducer.py")

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="runtime_delta_fault" \
    -D mapreduce.job.maps=24 \
    -D mapreduce.job.reduces=8 \
    -input {input_path} \
    -output {output_path} \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file {mapper_path} \
    -file {reducer_path}
"""

process = run_background(cmd)

print("  任务已启动，等待任务初始化...")
time.sleep(5)

print("\n▶ 查找正在运行的 MapReduce 任务...")

found = False
am_host = None
app_id = None
max_retries = 30

for i in range(max_retries):
    try:
        list_output = run("yarn application -list 2>/dev/null || true")
        running_lines = [
            line for line in list_output.splitlines()
            if "RUNNING" in line and ("runtime_delta" in line.lower() or "streamjob" in line.lower())
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
        if "RUNNING" in line and ("runtime_delta" in line.lower() or "streamjob" in line.lower())
    ]
    if running_lines:
        app_id = running_lines[0].split()[0]
        print(f"  ✔ 找到任务: {app_id}")
        found = True

if not found:
    print("  ✘ 未找到运行中的任务，跳过故障注入")
    process.wait()
    sys.exit(0)

print(f"\n▶ 查询 ApplicationMaster 所在节点...")
status = run(f"yarn application -status {app_id}")

match = re.search(r"AM Host\s*:\s*([a-zA-Z0-9\-]+)", status)
if not match:
    print("  ✘ 未找到 AM Host 信息")
    process.wait()
    sys.exit(1)

am_host = match.group(1)
print(f"  ✔ AM 运行节点: {am_host}")

print(f"\n▶ 在 {am_host} 上查找 MRAppMaster PID...")

pid = run(
    f"ssh {am_host} \"ps -eo pid,ppid,cmd --sort=-rss | grep MRAppMaster | grep -v grep | head -n 1 | awk '{{print \\$1}}'\""
).strip()

if not pid:
    print(f"  ✘ 在 {am_host} 上找不到 MRAppMaster 主进程")
    process.wait()
    sys.exit(1)

print(f"  ✔ MRAppMaster PID: {pid}")

print(f"\n▶ 注入运行时间异常，挂起 AM {FAULT_DURATION} 秒...")

mark_fault_start("runtime_delta", {"target": "MRAppMaster", "host": am_host, "pid": pid, "duration": FAULT_DURATION})
mark_fault_injection("runtime_delta", f"{am_host}:{pid}", "SIGSTOP", FAULT_DURATION)

run(f"ssh {am_host} \"sudo kill -STOP {pid}\"")
print("  ✔ 已挂起 MRAppMaster")

time.sleep(FAULT_DURATION)

run(f"ssh {am_host} \"sudo kill -CONT {pid}\"")
print("  ✔ 已恢复 MRAppMaster")

mark_fault_injection("runtime_delta", f"{am_host}:{pid}", "SIGCONT", None)
mark_fault_end("runtime_delta", {"target": "MRAppMaster", "host": am_host, "pid": pid})

print("\n▶ 等待任务完成...")
process.wait()

print("\n" + "=" * 60)
print("🎉 运行时间异常故障注入完成")
print("=" * 60)
