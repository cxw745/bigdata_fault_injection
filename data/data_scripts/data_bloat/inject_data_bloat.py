#!/usr/bin/env python3
"""
数据膨胀故障注入

在MapReduce任务中，通过Mapper代码将输入数据膨胀输出，产生大量中间数据

效果：
- Map阶段输出数据量急剧增加
- Shuffle阶段网络传输压力增大
- 磁盘IO压力增大

参数（通过环境变量）：
- BLOAT_FACTOR: 膨胀倍数（默认4倍）
"""
import subprocess
import sys
sys.stdout.reconfigure(line_buffering=True)
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "collect_data"))
from fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection


os.environ["PATH"] = "/opt/hadoop/bin:" + os.environ.get("PATH", "")
SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run(cmd):
    return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()

print("=" * 60)
print("数据膨胀故障注入 - Mapper数据膨胀")
print("=" * 60)


# 磁盘空间保护：注入前检查磁盘剩余空间
try:
    _df_out = subprocess.check_output('df / --output=pcent', shell=True).decode()
    _disk_pct = int(_df_out.strip().split('
')[-1].replace('%',''))
    if _disk_pct > 75:
        print(f'⚠ 磁盘使用率{_disk_pct}%超过75%阈值，跳过data_bloat注入')
        sys.exit(0)
except Exception as _e:
    print(f'⚠ 磁盘空间检查失败: {_e}')

BLOAT_FACTOR = int(os.environ.get("BLOAT_FACTOR", "4"))

print(f"\n▶ 故障参数:")
print(f"  膨胀倍数: {BLOAT_FACTOR}x")

hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
hadoop_cmd = f"{hadoop_home}/bin/hadoop"

input_path = "/HiBench/HiBench/Wordcount/Input"
output_path = "/user/hadoop/bloat_output"

print(f"\n▶ 清理旧输出...")
run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")

mapper_path = os.path.join(SCRIPTS_DIR, "data_bloat", "mapper_bloat.py")

print(f"\n▶ 启动 MapReduce 任务...")
print(f"  Mapper: {mapper_path}")

mark_fault_start("data_bloat", {"bloat_factor": BLOAT_FACTOR})

env = os.environ.copy()
env["BLOAT_FACTOR"] = str(BLOAT_FACTOR)

cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="data_bloat_fault" \
    -D mapreduce.job.maps=24 \
    -D mapreduce.job.reduces=8 \
    -inputformat org.apache.hadoop.mapred.SequenceFileInputFormat \
    -input {input_path} \
    -output {output_path} \
    -mapper "python3 mapper_bloat.py" \
    -reducer cat \
    -file {mapper_path} \
    -numReduceTasks 8 \
    -cmdenv BLOAT_FACTOR={BLOAT_FACTOR}
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
    
    print("\n▶ 验证数据膨胀效果...")
    try:
        job_list = run("yarn application -list -appStates FINISHED 2>/dev/null | tail -n 1")
        if job_list:
            job_id = job_list.split()[0]
            print(f"  任务ID: {job_id}")
            
            logs_cmd = f"yarn logs -applicationId {job_id} 2>/dev/null | grep 'Map output records' | tail -n 1"
            map_output = run(logs_cmd)
            if map_output:
                print(f"  {map_output}")
                
                mark_fault_injection("data_bloat", "map_output", map_output.strip(), None)
    except Exception as e:
        print(f"  无法获取任务日志: {e}")
    
    mark_fault_end("data_bloat", {"result": "success"})
else:
    print(f"\n⚠ 任务返回码: {process.returncode}")
    mark_fault_end("data_bloat", {"result": "completed", "returncode": process.returncode})

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
print("🎉 数据膨胀故障注入完成")
print("=" * 60)
