#!/usr/bin/env python3
"""
长尾任务故障注入

在MapReduce任务中，指定Map任务注入长尾延迟，导致整体任务完成时间变长

支持三种注入模式：
1. task_id: 基于任务ID确定性注入（推荐）
2. ratio: 基于比例精确控制
3. probability: 基于概率（旧方式，不推荐）
"""
import subprocess
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection

SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()

print("=" * 60)
print("长尾任务故障注入 - Mapper指定延迟")
print("=" * 60)

LONG_TAIL_MODE = os.environ.get("LONG_TAIL_MODE", "task_id")
LONG_TAIL_TASK_IDS = os.environ.get("LONG_TAIL_TASK_IDS", "0,5,10")
LONG_TAIL_RATIO = float(os.environ.get("LONG_TAIL_RATIO", "0.25"))
LONG_TAIL_DURATION = int(os.environ.get("LONG_TAIL_DURATION", "60"))

print(f"\n▶ 故障参数:")
print(f"  注入模式: {LONG_TAIL_MODE}")
print(f"  指定任务ID: {LONG_TAIL_TASK_IDS}")
print(f"  注入比例: {LONG_TAIL_RATIO * 100}%")
print(f"  延迟时长: {LONG_TAIL_DURATION}秒")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/long_tail_output"

print(f"\n▶ 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "long_tail", "mapper_long_tail.py")
reducer_path = os.path.join(SCRIPTS_DIR, "long_tail", "reducer_long_tail.py")

print(f"\n▶ 启动 MapReduce 任务...")
print(f"  Mapper: {mapper_path}")
print(f"  Reducer: {reducer_path}")

mark_fault_start("long_tail", {
    "mode": LONG_TAIL_MODE,
    "task_ids": LONG_TAIL_TASK_IDS,
    "ratio": LONG_TAIL_RATIO,
    "duration": LONG_TAIL_DURATION
})

env = os.environ.copy()
env["LONG_TAIL_MODE"] = LONG_TAIL_MODE
env["LONG_TAIL_TASK_IDS"] = LONG_TAIL_TASK_IDS
env["LONG_TAIL_RATIO"] = str(LONG_TAIL_RATIO)
env["LONG_TAIL_DURATION"] = str(LONG_TAIL_DURATION)

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="long_tail_fault" \
    -D mapreduce.job.maps=24 \
    -D mapreduce.job.reduces=8 \
    -D mapreduce.map.maxattempts=2 \
    -D mapreduce.reduce.maxattempts=2 \
    -input {input_path} \
    -output {output_path} \
    -mapper "python3 mapper_long_tail.py" \
    -reducer "python3 reducer_long_tail.py" \
    -file {mapper_path} \
    -file {reducer_path}
"""

process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)

for line in iter(process.stdout.readline, ''):
    print(line, end='')

process.wait()

if process.returncode == 0:
    print("\n✔ 任务完成")
    mark_fault_end("long_tail", {"result": "success"})
else:
    print(f"\n⚠ 任务返回码: {process.returncode}")
    mark_fault_end("long_tail", {"result": "completed", "returncode": process.returncode})

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
print("🎉 长尾任务故障注入完成")
print("=" * 60)
