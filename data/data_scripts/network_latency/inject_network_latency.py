#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
网络延迟故障注入脚本

使用chaosblade工具在网络接口上注入延迟
先启动MapReduce任务，然后注入网络延迟
"""

import subprocess
import time
import sys
import os
import json
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from collect_data.unified_config import get_slave_nodes, get_master_node, DEFAULT_FAULT_PARAMS
from collect_data.fault_marker import mark_fault_start, mark_fault_end, mark_fault_injection

CHAOSBLADE_PATH = "/opt/chaosblade-1.7.2/blade"
FAULT_DURATION = DEFAULT_FAULT_PARAMS.get("fault_duration", 60)
LATENCY_MS = 500
JITTER_MS = 20
NETWORK_INTERFACE = "ens3"

def run(cmd, check=True):
    """执行命令"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=check
        )
        return result.stdout.strip(), result.stderr.strip()
    except subprocess.CalledProcessError as e:
        return e.stdout.strip() if e.stdout else "", e.stderr.strip() if e.stderr else ""

def run_background(cmd):
    """后台运行命令"""
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def get_network_interface(node=None):
    """获取网络接口名称"""
    if node:
        stdout, _ = run(f"ssh {node} \"ip route | head -1 | awk '{{print \$5}}'\"", check=False)
        if stdout.strip():
            return stdout.strip()
    else:
        stdout, _ = run("ip route | head -1 | awk '{print $5}'", check=False)
        if stdout.strip():
            return stdout.strip()
    return NETWORK_INTERFACE

def inject_latency_with_chaosblade(node, latency_ms, jitter_ms, interface):
    """使用chaosblade在指定节点注入网络延迟"""
    print(f"  在 {node} 上注入网络延迟...")
    
    offset_ms = jitter_ms
    
    cmd = f"sudo {CHAOSBLADE_PATH} create network delay --time {latency_ms} --offset {offset_ms} --interface {interface} --force"
    
    if node != "localhost" and node != get_master_node():
        cmd = f"ssh {node} \"{cmd}\""
    
    stdout, stderr = run(cmd, check=False)
    
    uid = None
    if stdout:
        try:
            result = json.loads(stdout)
            if result.get("success"):
                uid = result.get("result", "").strip()
                print(f"    {node}: 已注入 {latency_ms}ms delay, UID: {uid}")
                return True, uid
            else:
                print(f"    {node}: chaosblade返回失败 - {result.get('error', 'unknown')}")
        except json.JSONDecodeError:
            uid_match = re.search(r'"result"\s*:\s*"([^"]+)"', stdout)
            if uid_match:
                uid = uid_match.group(1)
                print(f"    {node}: 已注入 {latency_ms}ms delay, UID: {uid}")
                return True, uid
    
    print(f"    {node}: 注入失败")
    print(f"    stdout: {stdout}")
    print(f"    stderr: {stderr}")
    return False, None

def destroy_latency_with_chaosblade(node, uid, interface):
    """使用chaosblade移除网络延迟"""
    if not uid:
        return True
    
    print(f"  在 {node} 上移除网络延迟 (UID: {uid})...")
    
    cmd = f"sudo {CHAOSBLADE_PATH} destroy {uid}"
    
    if node != "localhost" and node != get_master_node():
        cmd = f"ssh {node} \"{cmd}\""
    
    stdout, stderr = run(cmd, check=False)
    
    if stdout:
        try:
            result = json.loads(stdout)
            if result.get("success"):
                print(f"    {node}: 已移除网络延迟")
                return True
        except json.JSONDecodeError:
            pass
    
    print(f"    {node}: 移除可能失败")
    return True

def check_chaosblade_available(node=None):
    """检查chaosblade是否可用"""
    cmd = f"test -x {CHAOSBLADE_PATH} && echo 'ok'"
    if node and node != "localhost" and node != get_master_node():
        cmd = f"ssh {node} \"{cmd}\""
    
    stdout, _ = run(cmd, check=False)
    return "ok" in stdout

def start_mapreduce_job():
    """启动MapReduce任务"""
    print("\n▶ 启动 MapReduce 任务...")
    
    hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
    hadoop_cmd = f"{hadoop_home}/bin/hadoop"
    
    input_path = "/HiBench/HiBench/Wordcount/Input"
    output_path = "/user/hadoop/network_latency_output"
    
    # 清理旧输出
    run(f"{hadoop_cmd} fs -rm -r {output_path} 2>/dev/null || true")
    
    # 使用通用mapper/reducer
    scripts_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    mapper_path = os.path.join(scripts_dir, "common_mapreduce", "mapper.py")
    reducer_path = os.path.join(scripts_dir, "common_mapreduce", "reducer.py")
    
    cmd = f"""
{hadoop_cmd} jar {hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
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
    print("  任务已启动")
    
    # 等待任务初始化
    time.sleep(10)
    
    return process

def wait_for_running_job():
    """等待任务进入RUNNING状态"""
    print("\n▶ 等待任务进入RUNNING状态...")
    
    for i in range(30):
        try:
            list_output, _ = run("yarn application -list 2>/dev/null || true", check=False)
            running_lines = [
                line for line in list_output.splitlines()
                if "RUNNING" in line
            ]
            
            if running_lines:
                app_id = running_lines[0].split()[0]
                print(f"  ✔ 任务正在运行: {app_id}")
                return app_id
        except:
            pass
        
        time.sleep(2)
    
    print("  ⚠ 未检测到RUNNING状态，继续执行故障注入...")
    return None

def main():
    print("=" * 60)
    print("网络延迟故障注入 (使用chaosblade)")
    print("=" * 60)
    
    # 1. 启动MapReduce任务
    mr_process = start_mapreduce_job()
    
    # 2. 等待任务进入RUNNING状态
    app_id = wait_for_running_job()
    
    # 3. 获取目标节点
    slave_nodes = get_slave_nodes()
    master_node = get_master_node()
    
    target_nodes = slave_nodes.copy()
    
    if len(sys.argv) > 1:
        specified_nodes = [n for n in sys.argv[1:] if n in slave_nodes or n == master_node]
        if specified_nodes:
            target_nodes = specified_nodes
    
    print(f"\n目标节点: {', '.join(target_nodes)}")
    print(f"延迟设置: {LATENCY_MS}ms")
    print(f"持续时间: {FAULT_DURATION}秒")
    
    # 4. 检查chaosblade可用性
    print("\n 检查chaosblade可用性...")
    available_nodes = []
    for node in target_nodes:
        if check_chaosblade_available(node):
            available_nodes.append(node)
            print(f"   {node}: chaosblade可用")
        else:
            print(f"   {node}: chaosblade不可用，跳过")
    
    if not available_nodes:
        print(" 没有可用的节点，退出")
        mr_process.wait()
        sys.exit(1)
    
    # 5. 注入网络延迟
    print("\n 开始注入网络延迟...")
    
    mark_fault_start("network_latency", {
        "target_nodes": available_nodes,
        "latency_ms": LATENCY_MS,
        "jitter_ms": JITTER_MS,
        "duration": FAULT_DURATION
    })
    
    injected_nodes = []
    uid_map = {}
    
    for node in available_nodes:
        interface = get_network_interface(node)
        success, uid = inject_latency_with_chaosblade(node, LATENCY_MS, JITTER_MS, interface)
        if success:
            injected_nodes.append(node)
            if uid:
                uid_map[node] = uid
            mark_fault_injection("network_latency", node, "network_delay", FAULT_DURATION)
    
    if not injected_nodes:
        print(" 所有节点注入失败")
        mr_process.wait()
        sys.exit(1)
    
    # 6. 等待故障持续时间
    print(f"\n 等待 {FAULT_DURATION} 秒...")
    print("   (按Ctrl+C可提前终止)")
    
    try:
        time.sleep(FAULT_DURATION)
    except KeyboardInterrupt:
        print("\n  收到中断信号，提前终止...")
    
    # 7. 移除网络延迟
    print("\n 移除网络延迟...")
    for node in injected_nodes:
        uid = uid_map.get(node)
        interface = get_network_interface(node)
        destroy_latency_with_chaosblade(node, uid, interface)
        mark_fault_injection("network_latency", node, "remove_delay", None)
    
    mark_fault_end("network_latency", {"injected_nodes": injected_nodes})
    
    # 8. 等待MapReduce任务完成
    print("\n▶ 等待MapReduce任务完成...")
    mr_process.wait()
    
    print("\n" + "=" * 60)
    print(" 网络延迟故障注入完成")
    print("=" * 60)

if __name__ == "__main__":
    main()
