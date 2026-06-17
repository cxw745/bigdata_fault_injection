#!/usr/bin/env python3
"""
数据倾斜故障注入

在MapReduce任务中，通过Mapper代码使80%的数据输出相同的key，导致单个Reducer处理大部分数据

效果：
- 单个Reducer负载过重
- 任务执行时间变长
- 数据分布不均匀
"""
import subprocess
import sys
sys.stdout.reconfigure(line_buffering=True)
import os
import random

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection


os.environ["PATH"] = "/opt/hadoop/bin:" + os.environ.get("PATH", "")
SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()

print("=" * 60)
print("数据倾斜故障注入 - Key Skew")
print("=" * 60)

SKEW_RATIO = float(os.environ.get("SKEW_RATIO", "0.99"))

print(f"\n▶ 故障参数:")
print(f"  倾斜比例: {SKEW_RATIO * 100}% 的词会被映射到同一个key")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/skew_output"

print(f"\n▶ 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "data_skew", "skew_mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "data_skew", "skew_reducer.py")

print(f"\n▶ 启动 MapReduce 任务...")
print(f"  Mapper: {mapper_path}")
print(f"  Reducer: {reducer_path}")

mark_fault_start("data_skew", {"skew_ratio": SKEW_RATIO})

env = os.environ.copy()
env["SKEW_RATIO"] = str(SKEW_RATIO)

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="data_skew_fault" \
    -D mapreduce.job.maps=24 \
    -D mapreduce.job.reduces=8 \
    -inputformat org.apache.hadoop.mapred.SequenceFileInputFormat \
    -input {input_path} \
    -output {output_path} \
    -mapper "python3 skew_mapper.py" \
    -reducer "python3 skew_reducer.py" \
    -file {mapper_path} \
    -file {reducer_path} \
    -cmdenv SKEW_RATIO={SKEW_RATIO}
"""

process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)

for line in iter(process.stdout.readline, ''):
    print(line, end='')

try:
    process.wait(timeout=300)
except subprocess.TimeoutExpired:
    print("\n⚠ MapReduce任务超时(300s)，强制终止")
    process.kill()
    process.wait(timeout=5)

if process.returncode == 0:
    print("\n✔ 任务完成")
    mark_fault_end("data_skew", {"result": "success"})
else:
    print(f"\n⚠ 任务返回码: {process.returncode}")
    mark_fault_end("data_skew", {"result": "completed", "returncode": process.returncode})

print("\n▶ 查看输出结果...")
try:
    result = run(f"{hadoop_cmd} fs -ls {output_path} 2>/dev/null || true")
    if result:
        print("  输出目录内容:")
        for line in result.split('\n')[:5]:
            print(f"    {line}")

        print("\n  部分结果预览:")
        preview = run(f"{hadoop_cmd} fs -text {output_path}/part-* 2>/dev/null | head -n 10 || true")
        for line in preview.split('\n')[:10]:
            if line.strip():
                print(f"    {line}")
except Exception as e:
    print(f"  无法读取输出: {e}")

print("\n" + "=" * 60)
print("🎉 数据倾斜故障注入完成")
print("=" * 60)