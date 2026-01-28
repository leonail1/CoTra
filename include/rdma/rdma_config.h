#pragma once

/* ============================================
 * RDMA Configuration for CoTra
 * ============================================
 * 
 * 支持两种RDMA模式：
 *   1. 标准Mellanox RDMA (默认)
 *   2. 阿里云eRDMA (通过CMake -DUSE_ERDMA=ON启用)
 * 
 * CMake构建示例：
 *   标准RDMA:  cmake -DMACHINE_NUM=16 ..
 *   eRDMA:     cmake -DUSE_ERDMA=ON -DMACHINE_NUM=2 ..
 */

/* ============================================
 * 集群配置 (通过CMake设置)
 * ============================================ */
#ifdef MACHINE_NUM_CMAKE
  #define MACHINE_NUM (MACHINE_NUM_CMAKE)
#else
  #define MACHINE_NUM (2)  // 默认2节点
#endif

#define MAX_THREAD_NUM (16)
#define GROUP_SIZE (16)
// NOTE: MAX_READ_NUM 是单线程向单机器发送的最大读取数
#define MAX_READ_NUM (2 * GROUP_SIZE)
#define MAX_VEC_LEN (2560)
#define MAX_QUERYBUFFER_SIZE (24576)
#define MAX_SEND_SGE (1)
#define MAX_RECV_SGE (1)
#define MAX_SGE_NUM (16)
// WARN: 大数据集可能需要增大此值
#define MAX_CACHE_VEC (2000)

#ifdef USE_ERDMA
  // eRDMA 兼容模式下的较小参数
  // 注意: max_send_wr = (MAX_WRITE_NUM + MAX_READ_NUM) * MACHINE_NUM
  //       max_recv_wr = MAX_WRITE_NUM * MACHINE_NUM
  // eRDMA QP 限制较严格 (通常 <= 128)
  #define MAX_WRITE_NUM (2)
  #define MAX_RECV_NUM (1)
#else
  // 标准 Mellanox RDMA 参数
  #define MAX_WRITE_NUM (512)
  #define MAX_RECV_NUM (64)
#endif
#define RELEASE_BLOCK (MAX_WRITE_NUM >> 2)
#define READ_MARK_OFF (25)
#define MAX_WC_NUM (64)
#define LOW_16BIT_MASK (0x0000ffff)

#define OFF (0)
#define ON (1)
#define DEF_PORT (18516)
#define DEF_IB_PORT (1)
#define LINK_UNSPEC (-2)
#define DEF_ITERS (1000)
#define DEF_GID_INDEX (-1)
#define DEF_INLINE (0)
#define MIN_RNR_TIMER (12)
#define DEF_QP_TIME (14)
#define DEF_CQ_MOD (100)
#define MSG_SIZE_CQ_MOD_LIMIT (8192)
#define DISABLED_CQ_MOD_VALUE (1)
#define MAX_SIZE (8388608)
#define DEF_INLINE_WRITE (220)
#define LAT_MEASURE_TAIL (2)
#define DEF_CACHE_LINE_SIZE (64)
#define SUCCESS (0)
#define FAILURE (1)

/* ============================================
 * eRDMA兼容性配置
 * 
 * 以下Mellanox特有功能在eRDMA上不可用：
 *   - HAVE_AES_XTS:   AES加密 (依赖mlx5dv)
 *   - HAVE_MLX5DV:    Mellanox Direct Verbs
 *   - HAVE_MLX5_DEVX: MLX5 DEVX支持
 *   - HAVE_EX_ODP:    Extended ODP
 *   - HAVE_PACKET_PACING, HAVE_RAW_ETH 等高级功能
 * ============================================ */

#ifdef USE_ERDMA
  /* eRDMA模式：禁用Mellanox特有功能 */
  /* #undef HAVE_AES_XTS */
  /* #undef HAVE_MLX5DV */
  /* #undef HAVE_MLX5_DEVX */
  /* #undef HAVE_DCS */
  /* #undef HAVE_EX_ODP */
  /* #undef HAVE_PACKET_PACING */
  /* #undef HAVE_RAW_ETH */
  /* #undef HAVE_RAW_ETH_REG */
  /* #undef HAVE_REG_DMABUF_MR */
  /* #undef HAVE_RO */
  /* #undef HAVE_SNIFFER */
  /* #undef HAVE_IPV4_EXT */
  /* #undef HAVE_IPV6 */
  /* #undef HAVE_XRCD */
#else
  /* 标准Mellanox RDMA模式 */
  #define HAVE_AES_XTS 1
  #define HAVE_MLX5DV 1
  #define HAVE_MLX5_DEVX 1
  #define HAVE_DCS 1
  #define HAVE_EX_ODP 1
  #define HAVE_PACKET_PACING 1
  #define HAVE_RAW_ETH 1
  #define HAVE_RAW_ETH_REG 1
  #define HAVE_REG_DMABUF_MR 1
  #define HAVE_RO 1
  #define HAVE_SNIFFER 1
  #define HAVE_IPV4_EXT 1
  #define HAVE_IPV6 1
  #define HAVE_XRCD 1
#endif

/* ============================================
 * 通用RDMA功能 (eRDMA和Mellanox都支持)
 * ============================================ */

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Enable endian conversion */
#define HAVE_ENDIAN 1

/* Have EX support */
#define HAVE_EX 1

/* Have a way to check gid type */
#define HAVE_GID_TYPE 1

/* API GID compatibility */
#define HAVE_GID_TYPE_DECLARED 1

/* Have new post send API support */
#define HAVE_IBV_WR_API 1

/* Define to 1 if you have the <infiniband/verbs.h> header file. */
#define HAVE_INFINIBAND_VERBS_H 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the `ibverbs' library (-libverbs). */
#define HAVE_LIBIBVERBS 1

/* Define to 1 if you have the `rdmacm' library (-lrdmacm). */
#define HAVE_LIBRDMACM 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the <pci/pci.h> header file. */
#define HAVE_PCI_PCI_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* ============================================
 * 未使用/禁用的功能
 * ============================================ */

/* Enable CUDA feature */
/* #undef CUDA_PATH */
/* #undef HAVE_CUDA */
/* #undef HAVE_CUDA_DMABUF */

/* HIP/ROCm support */
/* #undef HAVE_HIP_HIP_RUNTIME_API_H */
/* #undef HAVE_HIP_HIP_VERSION_H */
/* #undef HAVE_ROCM */
/* #undef __HIP_PLATFORM_AMD__ */

/* Habana Labs */
/* #undef HAVE_HL */
/* #undef HAVE_HLTHUNK_H */
/* #undef HAVE_MISC_HABANALABS_H */

/* Neuron */
/* #undef HAVE_NEURON */
/* #undef HAVE_NEURON_DMABUF */
/* #undef HAVE_NRT_NRT_H */
/* #undef HAVE_SYNAPSE_API_H */

/* Other */
/* #undef HAVE_OOO_ATTR */
/* #undef HAVE_SRD */
/* #undef HAVE_SRD_WITH_RDMA_READ */
/* #undef HAVE_SRD_WITH_RDMA_WRITE */
/* #undef HAVE_SCIF */
/* #undef IS_FREEBSD */

/* Define to the sub-directory where libtool stores uninstalled libraries. */
#define LT_OBJDIR ".libs/"

/* Package info */
#define PACKAGE "perftest"
#define PACKAGE_BUGREPORT "linux-rdma@vger.kernel.org"
#define PACKAGE_NAME "perftest"
#define PACKAGE_STRING "perftest 6.22"
#define PACKAGE_TARNAME "perftest"
#define PACKAGE_URL ""
#define PACKAGE_VERSION "6.22"
#define STDC_HEADERS 1
#define VERSION "6.22"
