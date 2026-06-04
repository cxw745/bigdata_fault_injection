#!/usr/bin/env python3
import subprocess, time, sys, re

FAULT_SECONDS = 120
WORKER_NODES = ["cpf-2","cpf-3","cpf-4"]

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True).strip()

print("▶ 查找正在运行的 MapReduce 作业 ...")
list_output = run("yarn application -list 2>/dev/null || true")

running_lines = [
    l for l in list_output.splitlines()
    if "RUNNING" in l and l.startswith("application_")
]

if not running_lines:
    print("✘ 没有 RUNNING 的 MR 作业")
    sys.exit(1)

app_id = running_lines[0].split()[0]
print(f"✔ 找到 Application: {app_id}")

status = run(f"yarn application -status {app_id}")

m = re.search(r"AM Container Host\s*:\s*([a-zA-Z0-9\-]+)", status)
candidate = m.group(1) if m else None

targets = []
if candidate:
    targets.append(candidate)
for n in WORKER_NODES:
    if n != candidate:
        targets.append(n)

for node in targets:
    print(f"▶ 在 {node} 上检测 NodeManager PID ...")

    try:
        pid = run(
            f"ssh {node} \"ps -ef | grep org.apache.hadoop.yarn.server.nodemanager.NodeManager | grep -v grep | awk '{{print $2}}' | head -n 1\""
        ).strip()
    except:
        pid = ""

    if not pid:
        print(f"  ✘ {node} 未找到 NodeManager 进程（跳过）")
        continue

    print(f"  ✔ NodeManager PID = {pid} ，暂停 {FAULT_SECONDS}s")
    run(f"ssh {node} \"sudo kill -STOP {pid}\"")
    print(f"  ✔ 已暂停 NodeManager @ {node}")

print(f"⏳ 等待 {FAULT_SECONDS} 秒 ...")
time.sleep(FAULT_SECONDS)

for node in targets:
    try:
        pid = run(
            f"ssh {node} \"ps -ef | grep org.apache.hadoop.yarn.server.nodemanager.NodeManager | grep -v grep | awk '{{print $2}}' | head -n 1\""
        ).strip()
    except:
        pid = ""

    if not pid:
        print(f"  ⚠ {node} 无 NodeManager 进程（略过恢复）")
        continue

    run(f"ssh {node} \"sudo kill -CONT {pid}\"")
    print(f"  ✔ 已恢复 NodeManager @ {node}")

print("🎉 exit_time 故障注入完成")
