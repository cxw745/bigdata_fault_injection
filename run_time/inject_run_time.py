#!/usr/bin/env python3
import subprocess
import time
import sys
import re

FAULT_TIME = 120  # 挂起秒数

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True).strip()

print("▶ 查找正在运行的 MapReduce 作业 ...")

list_output = run("yarn application -list 2>/dev/null || true")
running_lines = [
    line for line in list_output.splitlines()
    if "RUNNING" in line and line.startswith("application_")
]

while not running_lines:
    print("❌ 没有正在运行的任务")
    print("▶ 10 秒后重试 ...")
    time.sleep(10)
    print(list_output)

app_id = running_lines[0].split()[0]
print(f"✔ 找到任务 ApplicationID = {app_id}")

print("▶ 查询 ApplicationMaster 所在节点 ...")
status = run(f"yarn application -status {app_id}")

match = re.search(r"AM Host\s*:\s*([a-zA-Z0-9\-]+)", status)
if not match:
    print("❌ 未找到 AM Host 信息")
    print(status)
    sys.exit(1)

am_host = match.group(1)
print(f"✔ AM 运行节点 = {am_host}")

print(f"▶ 在 {am_host} 上查找 MRAppMaster PID ...")

# 只取占用内存最大的 MRAppMaster 主进程
pid = run(
    f"ssh {am_host} \"ps -eo pid,ppid,cmd --sort=-rss | grep MRAppMaster | grep -v grep | head -n 1 | awk '{{print \\$1}}'\""
).strip()

if not pid:
    print(f"❌ 在 {am_host} 上找不到 MRAppMaster 主进程")
    sys.exit(1)

print(f"✔ MRAppMaster 主进程 PID = {pid}")

print(f"▶ 注入运行时间异常 (runtime_delta)，挂起 AM {FAULT_TIME} 秒 ...")

run(f"ssh {am_host} \"sudo kill -STOP {pid}\"")
print("✔ 已挂起 MRAppMaster（任务会卡住）")

time.sleep(FAULT_TIME)

run(f"ssh {am_host} \"sudo kill -CONT {pid}\"")
print("✔ 已恢复 MRAppMaster（任务继续执行）")

print("🎉 故障注入完成")
