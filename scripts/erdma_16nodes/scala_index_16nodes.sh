#!/bin/bash
# ============================================
# 16节点分布式索引构建脚本 (eRDMA版本)
# ============================================
# 
# 功能：
#   1. 自动编译项目 (cmake + make)
#   2. 将编译产物和数据集同步到所有节点
#   3. 启动所有节点的索引构建进程
#   4. 远程节点日志重定向到本机
#
# 用法：
#   ./scala_index_16nodes.sh [dataset]
#   dataset: sift1m, sift100m (默认: sift1m)
#
# 前提条件：
#   - SSH免密登录已配置
#   - 所有节点有相同的目录结构 (/root/CoTra)
#
# 节点配置：
#   节点0 (leader): 172.25.128.1 (本机)
#   节点1-15 (member): 172.25.128.2 ~ 172.25.128.16
#

set -e  # 遇到错误立即退出

# 获取脚本所在目录和项目根目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "${SCRIPT_DIR}/../.." && pwd )"
BUILD_DIR="${PROJECT_ROOT}/build"

# ============================================
# 数据集选择
# ============================================
DATASET=${1:-sift1m}  # 默认使用sift1m

case "${DATASET}" in
    sift1m)
        dataset_path=/root/CoTra/data/sift1m
        million=1
        base_file=${dataset_path}/sift1m_base.bin
        data_type="float"
        ;;
    sift100m)
        dataset_path=/root/CoTra/data/sift100m
        million=100
        base_file=${dataset_path}/learn.${million}M.u8bin
        data_type="uint8"
        ;;
    *)
        echo "错误: 未知数据集 '${DATASET}'"
        echo "支持的数据集: sift1m, sift100m"
        exit 1
        ;;
esac

index_save_path=${dataset_path}

# 全局变量，用于清理
TAIL_PID=""
LOCAL_PID=""

# ============================================
# 错误处理：异常退出时自动清理
# ============================================

# 清理日志文件
CLEANUP_LOG="${PROJECT_ROOT}/logs/cleanup.log"

# 标记是否已经执行过清理
CLEANUP_DONE=0

do_cleanup() {
    local exit_code=${1:-1}
    
    # 防止重复清理
    if [ ${CLEANUP_DONE} -eq 1 ]; then
        return
    fi
    CLEANUP_DONE=1
    
    # 先杀掉 tail 进程
    if [ -n "${TAIL_PID}" ]; then
        kill ${TAIL_PID} 2>/dev/null || true
    fi
    
    echo ""
    echo "========================================"
    echo "错误: 脚本异常退出 (退出码: ${exit_code})"
    echo "正在启动后台清理进程..."
    echo "清理日志: ${CLEANUP_LOG}"
    echo "========================================"
    echo ""
    
    # 删除旧的清理日志
    rm -f "${CLEANUP_LOG}"
    
    # 使用 setsid 启动独立的后台进程执行清理
    # 这个进程与主脚本完全分离，不会受主脚本退出影响
    setsid bash "${SCRIPT_DIR}/stop_all.sh" </dev/null >"${CLEANUP_LOG}" 2>&1 &
    
    echo "清理进程已启动，主脚本退出"
}

cleanup_on_error() {
    local exit_code=$?
    
    if [ ${exit_code} -ne 0 ] && [ ${CLEANUP_DONE} -eq 0 ]; then
        do_cleanup ${exit_code}
    fi
}

# 捕获退出信号和常见终止信号，自动执行清理
trap cleanup_on_error EXIT
trap 'do_cleanup 130' INT
trap 'do_cleanup 143' TERM

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
REMOTE_USER="root"             # 远程节点用户名

# ============================================
# 构建参数
# ============================================
num_threads=4                                  # 线程数
R=48                                           # 图的最大度数
L=500                                          # 构建队列大小（相当于efConstruction）
MAKE_JOBS=$(nproc)                             # 编译并行度

# ============================================
# 日志目录
# ============================================
LOG_DIR="${PROJECT_ROOT}/logs"
mkdir -p ${LOG_DIR}

echo "========================================"
echo "16节点分布式索引构建 (eRDMA)"
echo "========================================"

# ============================================
# 步骤1: 编译项目
# ============================================
echo ""
echo "[步骤1] 编译项目..."
mkdir -p ${BUILD_DIR}
cd ${BUILD_DIR}

# CMake配置 (eRDMA模式，16节点)
echo "  -> CMake配置..."
cmake -DUSE_ERDMA=ON -DMACHINE_NUM=16 ..

# Make编译
echo "  -> Make编译 (${MAKE_JOBS}线程)..."
make -j${MAKE_JOBS}

echo "  -> 编译完成"

# ============================================
# 步骤2: 同步编译产物和数据集到远程节点
# ============================================
echo ""
echo "[步骤2] 同步文件到远程节点..."

for REMOTE_NODE in "${REMOTE_NODES[@]}"; do
    echo "  -> 同步到 ${REMOTE_NODE}..."
    
    # 确保远程目录存在
    ssh ${REMOTE_USER}@${REMOTE_NODE} "mkdir -p ${BUILD_DIR}/tests ${PROJECT_ROOT}/scripts/erdma_16nodes"
    
    # 同步编译产物 (scala_index可执行文件)
    echo "    - 复制编译产物..."
    rsync -az ${BUILD_DIR}/tests/scala_index ${REMOTE_USER}@${REMOTE_NODE}:${BUILD_DIR}/tests/
    
    # 同步配置文件
    echo "    - 复制配置文件..."
    rsync -az ${SCRIPT_DIR}/ ${REMOTE_USER}@${REMOTE_NODE}:${SCRIPT_DIR}/
    
    # 检查并同步数据集
    echo "    - 检查数据集..."
    REMOTE_DATA_EXISTS=$(ssh ${REMOTE_USER}@${REMOTE_NODE} "[ -f ${base_file} ] && echo 'yes' || echo 'no'")
    if [ "${REMOTE_DATA_EXISTS}" = "no" ]; then
        echo "    - 数据集不存在，开始复制 (这可能需要较长时间)..."
        ssh ${REMOTE_USER}@${REMOTE_NODE} "mkdir -p ${dataset_path}"
        rsync -az --progress ${dataset_path}/ ${REMOTE_USER}@${REMOTE_NODE}:${dataset_path}/
        echo "    - 数据集复制完成"
    else
        echo "    - 数据集已存在，跳过"
    fi
    
    echo "  -> ${REMOTE_NODE} 同步完成"
done

# ============================================
# 步骤3: 重启memcached
# ============================================
echo ""
echo "[步骤3] 重启memcached..."
source "${PROJECT_ROOT}/scripts/restart_memcache.sh"
CONF_FILE="${SCRIPT_DIR}/ip_16nodes.conf"
clear_memcache "$CONF_FILE"

# ============================================
# 步骤4: 准备索引目录
# ============================================
echo ""
echo "[步骤4] 准备索引目录..."
index_save_dir=${index_save_path}/scalagraph/erdma_${million}M_16P
mkdir -p ${index_save_dir}

# 远程节点也创建目录
for REMOTE_NODE in "${REMOTE_NODES[@]}"; do
    ssh ${REMOTE_USER}@${REMOTE_NODE} "mkdir -p ${index_save_dir}"
done

echo ""
echo "========================================"
echo "开始构建分布式索引"
echo "数据集: ${DATASET}"
echo "数据文件: ${base_file}"
echo "数据类型: ${data_type}"
echo "数据规模: ${million}M"
echo "线程数: ${num_threads}"
echo "内存预算: 24GB"
echo "RDMA设备: erdma_0"
echo "本机节点: ${LOCAL_NODE}"
echo "远程节点: ${REMOTE_NODES[*]}"
echo "========================================"

# 构建 scala_index 命令
SCALA_CMD="${BUILD_DIR}/tests/scala_index \
    --config_file ${CONF_FILE} \
    -d erdma_0 \
    --graph_type scalagraph_v3 \
    --data_type ${data_type} --dist_fn l2 \
    --data_path ${base_file} \
    --index_path_prefix ${index_save_dir}/merged_index \
    -R ${R} -L ${L} -B 100 -M 24 -T ${num_threads} \
    --scala_v3 -s ${million} -t ${num_threads}"

# ============================================
# 步骤5: 首先启动本机节点 (后台运行)
# ============================================
echo ""
echo "[步骤5] 启动本机节点..."
LOCAL_LOG="${LOG_DIR}/scala_index_node0.log"
echo "  -> 启动节点0 (${LOCAL_NODE})，日志: ${LOCAL_LOG}"

# 本机后台运行，日志输出到文件，同时tail显示
${SCALA_CMD} > ${LOCAL_LOG} 2>&1 &
LOCAL_PID=$!

# 等待本机进程启动
sleep 2

# ============================================
# 步骤6: 启动远程节点
# ============================================
echo ""
echo "[步骤6] 启动远程节点..."

REMOTE_PIDS=()
NODE_ID=1
for REMOTE_NODE in "${REMOTE_NODES[@]}"; do
    REMOTE_LOG="${LOG_DIR}/scala_index_node${NODE_ID}.log"
    echo "  -> 启动节点${NODE_ID} (${REMOTE_NODE})，日志: ${REMOTE_LOG}"
    
    # SSH到远程节点运行，stdout/stderr通过SSH管道重定向到本机日志文件
    ssh ${REMOTE_USER}@${REMOTE_NODE} "${SCALA_CMD}" > ${REMOTE_LOG} 2>&1 &
    REMOTE_PIDS+=($!)
    
    NODE_ID=$((NODE_ID + 1))
done

# ============================================
# 步骤7: 等待所有进程完成，实时显示本机日志
# ============================================
echo ""
echo "[步骤7] 等待索引构建完成..."
echo "  -> 实时显示本机日志 (Ctrl+C 可中断显示，不影响后台进程)"
echo ""

# 实时显示本机日志
tail -f ${LOCAL_LOG} &
TAIL_PID=$!

# 等待本机进程完成，捕获退出码
set +e  # 临时禁用 set -e
wait ${LOCAL_PID}
LOCAL_EXIT_CODE=$?
set -e  # 恢复 set -e

# 停止tail
kill ${TAIL_PID} 2>/dev/null || true
wait ${TAIL_PID} 2>/dev/null || true
TAIL_PID=""

# 等待所有远程进程完成
for pid in "${REMOTE_PIDS[@]}"; do
    wait ${pid} 2>/dev/null || true
done

# 检查本机进程退出码
if [ ${LOCAL_EXIT_CODE} -ne 0 ]; then
    echo ""
    echo "========================================"
    echo "本机进程异常退出 (退出码: ${LOCAL_EXIT_CODE})"
    echo "查看日志: ${LOCAL_LOG}"
    echo "========================================"
    # 先执行清理，再退出（确保清理在 exit 之前完成）
    do_cleanup ${LOCAL_EXIT_CODE}
    exit ${LOCAL_EXIT_CODE}
fi

echo ""
echo "========================================"
echo "索引构建完成"
echo "索引保存路径: ${index_save_dir}"
echo "日志目录: ${LOG_DIR}"
echo "========================================"
