#!/usr/bin/env python3
"""
任务失败故障注入

在MapReduce任务中，指定Map任务抛出RuntimeException，导致任务失败重试

支持三种注入模式：
1. task_id: 基于任务ID确定性注入（推荐）
2. ratio: 基于比例精确控制
3. probability: 基于概率（旧方式，不推荐）
"""
import subprocess
import time
import sys
sys.stdout.reconfigure(line_buffering=True)
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection


os.environ["PATH"] = "/opt/hadoop/bin:" + os.environ.get("PATH", "")
SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()

def run_background(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

print("=" * 60)
print("任务失败故障注入 - Mapper指定失败")
print("=" * 60)

TASK_FAIL_MODE = os.environ.get("TASK_FAIL_MODE", "ratio")
TASK_FAIL_TASK_IDS = os.environ.get("TASK_FAIL_TASK_IDS", "0,5,10")
TASK_FAIL_RATIO = float(os.environ.get("TASK_FAIL_RATIO", "0.8"))

print(f"\n▶ 故障参数:")
print(f"  注入模式: {TASK_FAIL_MODE}")
print(f"  指定任务ID: {TASK_FAIL_TASK_IDS}")
print(f"  注入比例: {TASK_FAIL_RATIO * 100}%")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/task_fail_output"

print(f"\n▶ 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "task_fail", "mapper_task_fail.py")
reducer_path = os.path.join(SCRIPTS_DIR, "task_fail", "reducer_task_fail.py")

print(f"\n▶ 启动 MapReduce 任务...")
print(f"  Mapper: {mapper_path}")
print(f"  Reducer: {reducer_path}")

mark_fault_start("task_fail", {
    "mode": TASK_FAIL_MODE,
    "task_ids": TASK_FAIL_TASK_IDS,
    "ratio": TASK_FAIL_RATIO
})

env = os.environ.copy()
env["TASK_FAIL_MODE"] = TASK_FAIL_MODE
env["TASK_FAIL_TASK_IDS"] = TASK_FAIL_TASK_IDS
env["TASK_FAIL_RATIO"] = str(TASK_FAIL_RATIO)

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="task_fail_fault" \
    -D mapreduce.job.maps=24 \
    -D mapreduce.job.reduces=8 \
    -D mapreduce.map.maxattempts=4 \
    -D mapreduce.reduce.maxattempts=4 \
    -inputformat org.apache.hadoop.mapred.SequenceFileInputFormat \
    -input {input_path} \
    -output {output_path} \
    -mapper "python3 mapper_task_fail.py" \
    -reducer "python3 reducer_task_fail.py" \
    -file {mapper_path} \
    -file {reducer_path}
"""

process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)

stdout_lines = []
for line in iter(process.stdout.readline, ''):
    stdout_lines.append(line)
    print(line, end='')

try:
    process.wait(timeout=300)
except subprocess.TimeoutExpired:
    print("\n⚠ MapReduce任务超时(300s)，强制终止")
    process.kill()
    process.wait(timeout=5)

if process.returncode == 0:
    print("\n✔ 任务完成")
    mark_fault_end("task_fail", {"result": "success", "returncode": process.returncode})
else:
    print(f"\n⚠ 任务返回码: {process.returncode}")
    mark_fault_end("task_fail", {"result": "completed_with_failures", "returncode": process.returncode})

print("\n▶ 查看输出结果...")
try:
    result = run(f"{hadoop_cmd} fs -ls {output_path} 2>/dev/null || true")
    if result:
        print("  输出目录内容:")
        for line in result.split('\n')[:5]:
            print(f"    {line}")
except Exception as e:
    print(f"  无法读取输出: {e}")

print("\n" + "=" * 60)
print("🎉 任务失败故障注入完成")
print("=" * 60)
