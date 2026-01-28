
#include <omp.h>
#include <signal.h>
#include <execinfo.h>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

#include <boost/program_options.hpp>

#include "coromem/include/share_mem.h"
#include "coromem/include/utils.h"
#include "index/scala_build.h"
#include "index/top_index.h"
#include "index/vamana/vamana.h"

// ============================================
// 信号处理：打印崩溃堆栈
// ============================================
void print_stacktrace(int sig) {
    void *array[50];
    int size;
    
    // 获取堆栈帧
    size = backtrace(array, 50);
    
    // 打印错误信息
    const char* sig_name = "UNKNOWN";
    switch(sig) {
        case SIGSEGV: sig_name = "SIGSEGV (段错误)"; break;
        case SIGABRT: sig_name = "SIGABRT (异常终止)"; break;
        case SIGFPE:  sig_name = "SIGFPE (浮点异常)"; break;
        case SIGILL:  sig_name = "SIGILL (非法指令)"; break;
        case SIGBUS:  sig_name = "SIGBUS (总线错误)"; break;
    }
    
    fprintf(stderr, "\n");
    fprintf(stderr, "========================================\n");
    fprintf(stderr, "程序崩溃! 信号: %s (%d)\n", sig_name, sig);
    fprintf(stderr, "堆栈跟踪 (%d 帧):\n", size);
    fprintf(stderr, "========================================\n");
    
    // 打印堆栈跟踪
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    
    fprintf(stderr, "========================================\n");
    fprintf(stderr, "请使用 addr2line 获取详细信息:\n");
    fprintf(stderr, "  addr2line -e build/tests/scala_index -Cf <地址>\n");
    fprintf(stderr, "========================================\n");
    
    // 恢复默认处理并重新触发信号（生成core dump）
    signal(sig, SIG_DFL);
    raise(sig);
}

void register_signal_handlers() {
    signal(SIGSEGV, print_stacktrace);  // 段错误
    signal(SIGABRT, print_stacktrace);  // abort()
    signal(SIGFPE,  print_stacktrace);  // 浮点异常
    signal(SIGILL,  print_stacktrace);  // 非法指令
    signal(SIGBUS,  print_stacktrace);  // 总线错误
    fprintf(stderr, "[调试] 信号处理器已注册\n");
}

int main(int argc, char **argv) {
  // 注册信号处理器，捕获崩溃并打印堆栈
  register_signal_handlers();
  
  SharedMem coromem;
  commandLine cmd(argc, argv, "Usage : \n");
  RdmaParameter rdma_param(cmd);
  auto &tp = getThreadPool();
  tp.poolPause();

  IndexParameter index_param(argc, argv);

  ScalaBuild scala_build(index_param, rdma_param);

  // Start rdma server.
  scala_build.init_rdma();

  // Start to do partition and dispatch.
  scala_build.partition();

  
  // Build local partition index.
  scala_build.swap_partition_info();

  scala_build.build();


  // [For Test] test in single machine 
  // if(rdma_param.machine_id == 0){
  //   scala_build.single_build();
  // }
  // else {
  //   return 0;
  // }
  
  tp.poolContiue();

  if (index_param.graph_type == GraphType::SCALA_V3){
    scala_build.write_partition_info();

    // Load partiiton.
    scala_build.partition_checkpoint();

    // [For Test] merge in single machine for test.
    // scala_build.single_merge();

    // Merge graph from other machine.
    scala_build.merge();

    // Now reformat b1 graph to scalagraph v3.
    scala_build.transfer_to_scala_v3();


    // [For Test] verify scala_v3.
    // if (index_param.machine_id == 0) {
    //   scala_build.verify_scala_v3();
    // }

    // [For Test] Vamana.
    if (index_param.machine_id == 0) {
      scala_build.merge_all_b1_index();
    }

    // build top index
    if (index_param.machine_id == 0) {
      scala_build.topindex_checkpoint();
      HNSWTopIndex top_index;
      top_index.build_hnsw_top_index(scala_build.index_param, cmd);
    }
  }
  else if (index_param.graph_type == GraphType::SharedNothing){
    scala_build.write_partition_info();
    // transfer to b2graph
    scala_build.shared_nothing_checkpoint();
    scala_build.trans_to_b2graph();

    // for K-means shard-nothing
    if (index_param.machine_id == 0) {
      scala_build.topindex_checkpoint();
      HNSWTopIndex top_index;
      top_index.build_hnsw_top_index(scala_build.index_param, cmd);
    }
  }
  
  // Delete temp files.
  // scala_build.delete_temp_files();
  return 0;
}