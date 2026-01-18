#!/usr/bin/env python3
import subprocess
import time
import re
import sys

# 挂起时间（秒）
SUSPEND_SECONDS = 120

def run(cmd, check=True):
    """运行命令并返回输出"""
    try:
        result = subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()
        return result
    except subprocess.CalledProcessError as e:
        if check:
            print(f"❌ 命令执行失败: {cmd}")
            print(f"   输出: {e.output}")
            sys.exit(1)
        return ""

def get_running_app():
    """获取当前运行的 MapReduce ApplicationID"""
    print("📋 查询运行中的应用...")
    output = run("yarn application -list")
    print(f"原始输出:\n{output}\n")
    
    # 过滤 RUNNING 状态的行，跳过表头
    lines = [line for line in output.splitlines() 
             if "RUNNING" in line and "application_" in line]
    
    if not lines:
        print("❌ 没有正在运行的 MapReduce 作业")
        sys.exit(1)
    
    # 解析 ApplicationID（通常是第一列）
    app_id = lines[0].split()[0]
    print(f"✔ 找到任务 ApplicationID = {app_id}")
    return app_id

def get_task_containers_v1(app_id):
    """方法1: 使用 yarn container -list（新版本）"""
    print(f"📦 尝试方法1: yarn container -list {app_id}")
    try:
        output = run(f"yarn container -list {app_id}", check=False)
        if not output or "doesn't exist" in output or "not found" in output:
            return []
        
        lines = [line for line in output.splitlines() 
                 if "RUNNING" in line and "container_" in line]
        print(f"找到 {len(lines)} 个容器")
        
        task_info = []
        for line in lines:
            parts = line.split()
            if len(parts) < 4:
                continue
            container_id = parts[0]
            node_host = parts[3]  # NODEID
            print(f"  容器: {container_id} on {node_host}")
            task_info.append((container_id, node_host))
        return task_info
    except Exception as e:
        print(f"⚠ 方法1 失败: {e}")
        return []

def get_task_containers_v2(app_id):
    """方法2: 使用 yarn logs 或 ResourceManager REST API 查询"""
    print(f"📦 尝试方法2: 通过 ResourceManager REST API 查询")
    try:
        # 获取 RM 地址
        rm_info = run("yarn resourcemanager -format-addresses", check=False)
        
        # 使用 REST API 查询应用信息
        output = run(f"curl -s http://localhost:8088/ws/v1/cluster/apps/{app_id}", check=False)
        print(f"REST API 响应: {output[:200]}")
        
        # 简单解析 JSON（如果可用）
        if "app" in output:
            return [(app_id, "unknown")]
    except Exception as e:
        print(f"⚠ 方法2 失败: {e}")
    return []

def get_task_containers_v3(app_id):
    """方法3: 查询 NodeManager 的容器状态"""
    print(f"📦 尝试方法3: 查询 NodeManager 容器")
    try:
        # 获取所有 NodeManager 的容器列表
        output = run("curl -s http://localhost:8042/ws/v1/node/containers", check=False)
        if "containers" in output:
            print(f"获得 NodeManager 容器列表")
            return [(app_id, "localhost")]
    except Exception as e:
        print(f"⚠ 方法3 失败: {e}")
    return []

def get_jvm_pids_for_app(app_id, nodes):
    """通过 jps 和应用ID查找 JVM 进程"""
    print(f"🔍 查找应用 {app_id} 的 JVM 进程...")
    task_info = []
    
    # 如果指定了节点，先在那些节点查找；否则查询所有 NodeManager
    if not nodes:
        print("⚠ 没有指定节点，尝试在本地查找...")
        nodes = ["localhost"]
    
    for node in nodes:
        try:
            if node == "localhost":
                jps_output = run("jps -l", check=False)
            else:
                jps_output = run(f"ssh {node} jps -l", check=False)
            
            print(f"  {node} 上的 JVM 进程:\n{jps_output}")
            
            # 查找包含 Task 或应用 ID 的进程
            for line in jps_output.splitlines():
                if "Task" in line or app_id in line or "hadoop" in line.lower():
                    parts = line.split()
                    if len(parts) >= 2:
                        pid = parts[0]
                        print(f"    ✔ 找到进程 PID {pid} on {node}")
                        task_info.append((node, pid))
        except Exception as e:
            print(f"  ⚠ 查询 {node} 失败: {e}")
    
    return task_info

def suspend_tasks(task_info):
    """挂起 Task 进程"""
    if not task_info:
        print("❌ 没有找到要挂起的进程")
        return
    
    print(f"▶ 注入任务退出时间异常故障 (exit_time)，挂起 Task {SUSPEND_SECONDS} 秒 ...")
    for node, pid in task_info:
        try:
            if node == "localhost":
                print(f"  挂起本地 Task PID {pid}")
                run(f"kill -STOP {pid}", check=False)
            else:
                print(f"  挂起 {node} 上的 Task PID {pid}")
                run(f"ssh {node} kill -STOP {pid}", check=False)
        except Exception as e:
            print(f"  ⚠ 挂起失败: {e}")
    
    print(f"✔ 已挂起 Task，等待 {SUSPEND_SECONDS} 秒 ...")
    time.sleep(SUSPEND_SECONDS)
    
    for node, pid in task_info:
        try:
            if node == "localhost":
                print(f"  恢复本地 Task PID {pid}")
                run(f"kill -CONT {pid}", check=False)
            else:
                print(f"  恢复 {node} 上的 Task PID {pid}")
                run(f"ssh {node} kill -CONT {pid}", check=False)
        except Exception as e:
            print(f"  ⚠ 恢复失败: {e}")
    
    print("🎉 故障注入结束，Task 已恢复")

if __name__ == "__main__":
    app_id = get_running_app()
    
    # 尝试多种方法查找容器
    containers = get_task_containers_v1(app_id)
    if not containers:
        containers = get_task_containers_v2(app_id)
    if not containers:
        containers = get_task_containers_v3(app_id)
    
    # 从容器信息提取节点列表
    nodes = list(set([node for _, node in containers])) if containers else []
    
    # 通过 jps 查找 JVM 进程
    task_info = get_jvm_pids_for_app(app_id, nodes)
    
    if task_info:
        suspend_tasks(task_info)
    else:
        print("❌ 没有找到 Task 容器 PID")
        print("💡 建议: 检查 'yarn application -list' 或 'jps' 输出是否正确")