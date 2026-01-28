#!/bin/bash
# ============================================
# 停止所有分布式索引构建进程 (16节点版本)
# ============================================
#
# 功能：
#   1. 停止本机和所有远程节点的 scala_index 进程
#   2. 停止 memcached 服务
#
# 节点配置：
#   节点0 (leader): 172.25.128.1 (本机)
#   节点1-15 (member): 172.25.128.2 ~ 172.25.128.16
#

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# ============================================
# 节点配置
# ============================================
LOCAL_NODE="172.25.128.1"
REMOTE_NODES=(
    "172.25.128.2"
    "172.25.128.3"
    "172.25.128.4"
    "172.25.128.5"
    "172.25.128.6"
    "172.25.128.7"
    "172.25.128.8"
    "172.25.128.9"
    "172.25.128.10"
    "172.25.128.11"
    "172.25.128.12"
    "172.25.128.13"
    "172.25.128.14"
    "172.25.128.15"
    "172.25.128.16"
)
REMOTE_USER="root"

echo "========================================"
echo "停止所有分布式索引构建进程 (16节点)"
echo "========================================"

# ============================================
# 步骤1: 停止本机 scala_index 进程
# ============================================
echo ""
echo "[步骤1] 停止本机进程..."

LOCAL_PIDS=$(pgrep -f "scala_index" 2>/dev/null || true)
if [ -n "${LOCAL_PIDS}" ]; then
    echo "  -> 发现本机 scala_index 进程: ${LOCAL_PIDS}"
    kill ${LOCAL_PIDS} 2>/dev/null || true
    sleep 1
    # 如果还没停止，强制杀死
    kill -9 ${LOCAL_PIDS} 2>/dev/null || true
    echo "  -> 本机进程已停止"
else
    echo "  -> 本机没有运行中的 scala_index 进程"
fi

# ============================================
# 步骤2: 停止远程节点 scala_index 进程
# ============================================
echo ""
echo "[步骤2] 停止远程节点进程..."

for REMOTE_NODE in "${REMOTE_NODES[@]}"; do
    echo "  -> 停止 ${REMOTE_NODE} 上的进程..."
    
    # 查找并杀死远程节点的 scala_index 进程
    ssh ${REMOTE_USER}@${REMOTE_NODE} "pkill -f scala_index 2>/dev/null || true" 2>/dev/null || true
    sleep 0.5
    ssh ${REMOTE_USER}@${REMOTE_NODE} "pkill -9 -f scala_index 2>/dev/null || true" 2>/dev/null || true
    
    echo "  -> ${REMOTE_NODE} 进程已停止"
done

# ============================================
# 步骤3: 停止 memcached
# ============================================
echo ""
echo "[步骤3] 停止 memcached..."

# 停止本机 memcached
if systemctl is-active --quiet memcached 2>/dev/null; then
    sudo systemctl stop memcached
    echo "  -> 本机 memcached 已停止"
else
    # 如果不是 systemd 管理，尝试直接杀进程
    pkill memcached 2>/dev/null || true
    echo "  -> 本机 memcached 已停止 (或未运行)"
fi

# ============================================
# 步骤4: 清理残留的 tail 进程
# ============================================
echo ""
echo "[步骤4] 清理残留进程..."
pkill -f "tail -f.*scala_index" 2>/dev/null || true
echo "  -> 清理完成"

echo ""
echo "========================================"
echo "所有进程已停止"
echo "========================================"
