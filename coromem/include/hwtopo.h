#ifndef COROGRAPH_HWTOPO_H
#define COROGRAPH_HWTOPO_H

#include <string>
#include <vector>

struct ThreadTopoInfo {
  unsigned tid;                  // thread id.
  unsigned socketLeader;         // first thread id in tid's socket
  unsigned socket;               // socket (L3 normally) of thread
  unsigned numaNode;             // memory bank.  may be different than socket.
  unsigned cumulativeMaxSocket;  // max socket id seen from [0, tid]
  unsigned osContext;            // OS ID to use for thread binding
  unsigned osNumaNode;           // OS ID for numa node
};

struct MachineTopoInfo {
  unsigned maxThreads;
  unsigned maxCores;
  unsigned maxSockets;
  unsigned maxNumaNodes;
};

struct HWTopoInfo {
  MachineTopoInfo machineTopoInfo;
  std::vector<ThreadTopoInfo> threadTopoInfo;

  void show();
};

struct cpuinfo {
  // fields filled in from OS files
  unsigned proc;
  unsigned physid;
  unsigned sib;
  unsigned coreid;
  unsigned cpucores;
  unsigned numaNode;  // from libnuma
  bool valid;         // from cpuset
  bool smt;           // computed
};

/**
 * getHWTopo determines the machine topology from the process information
 * exposed in /proc and /dev filesystems.
 */
HWTopoInfo getHWTopo();

/**
 * parseCPUList parses cpuset information in "List format" as described in
 * cpuset(7) and available under /proc/self/status
 */
std::vector<int> parseCPUList(const std::string &in);

/**
 * bindThreadSelf binds a thread to an osContext as returned by getHWTopo.
 */
bool bindThreadSelf(unsigned osContext);

bool unbindThreadSelf();

#endif
