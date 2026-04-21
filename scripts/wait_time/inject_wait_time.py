#!/usr/bin/env python3
import subprocess
import time
import sys

RM_HOST = "cpf-1"
FAULT_DURATION = 120

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True).strip()

print("▶ 查找 ResourceManager JVM 进程 PID ...")

# 只匹配真正的 RM 主类
cmd = (
    f"ssh {RM_HOST} "
    "\"ps -eo pid,cmd | "
    "grep '[o]rg.apache.hadoop.yarn.server.resourcemanager.ResourceManager'\""
)

out = run(cmd)

if not out:
    print("✘ 未找到 ResourceManager JVM 进程")
    sys.exit(1)

lines = out.splitlines()
if len(lines) != 1:
    print("✘ 匹配到多个 RM 进程，拒绝注入以避免误杀：")
    print(out)
    sys.exit(1)

pid = lines[0].split()[0]
print(f"✔ ResourceManager JVM PID = {pid}")

# 再确认一次 PID 是否还活着
try:
    run(f"ssh {RM_HOST} \"kill -0 {pid}\"")
except subprocess.CalledProcessError:
    print(f"✘ PID {pid} 已不存在，终止")
    sys.exit(1)

print(f"▶ 注入等待时间异常 (fault=wait_time)，挂起 RM {FAULT_DURATION}s")

run(f"ssh {RM_HOST} \"sudo kill -STOP {pid}\"")
print("✔ RM 已挂起")

time.sleep(FAULT_DURATION)

run(f"ssh {RM_HOST} \"sudo kill -CONT {pid}\"")
print("✔ RM 已恢复")

print("🎉 wait_time 故障注入完成")
