#!/usr/bin/env python3
"""
通用 MapReduce WordCount 任务

标准的 WordCount 实现，用于正常运行或作为基准对比
"""
import subprocess
import sys
sys.stdout.reconfigure(line_buffering=True)
import os


os.environ["PATH"] = "/opt/hadoop/bin:" + os.environ.get("PATH", "")
SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()

print("=" * 60)
print("通用 MapReduce WordCount 任务")
print("=" * 60)

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/wordcount_output"

print(f"\n▶ 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "mapper.py")
reducer_path = os.path.join(SCRIPTS_DIR, "common_mapreduce", "reducer.py")

print(f"\n▶ 启动 MapReduce 任务...")
print(f"  Mapper: {mapper_path}")
print(f"  Reducer: {reducer_path}")

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="wordcount_benchmark" \
    -D mapreduce.job.maps=24 \
    -D mapreduce.job.reduces=8 \
    -inputformat org.apache.hadoop.mapred.SequenceFileInputFormat \
    -input {input_path} \
    -output {output_path} \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file {mapper_path} \
    -file {reducer_path}
"""

process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

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
else:
    print(f"\n⚠ 任务返回码: {process.returncode}")

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
print("🎉 WordCount 任务完成")
print("=" * 60)
