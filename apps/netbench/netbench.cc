extern "C" {
#include <base/log.h>
#include <net/ip.h>
#include <unistd.h>
}

#include "net.h"
#include "runtime.h"
#include "sync.h"
#include "synthetic_worker.h"
#include "thread.h"
#include "timer.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono;
// TODO girfan: Rename this to microsec.
using sec = duration<double, std::micro>;

// <- ARGUMENTS FOR EXPERIMENT ->
// the number of worker threads to spawn.
int threads;
// the remote TCP addresses of the servers.
std::vector<netaddr> servers;
// target packets per second.
double max_rps;
// percentage of write requests.
double write_pct;
// number of samples to increment in until target packets/second is achieved.
int samples;
// number of iterations required for 1us on target server.
constexpr uint64_t kIterationsPerUS = 65;
// Number of seconds to warmup at rate 0.
constexpr uint64_t kWarmupUpSeconds = 5;
// Server port to connect to.
constexpr uint64_t kNetbenchPort = 8001;

// Number of bytes in a page.
constexpr uint64_t PAGE_SIZE = 4096; // 4K page

constexpr uint64_t kUptimePort = 8002;
constexpr uint64_t kUptimeMagic = 0xDEADBEEF;
struct uptime {
  uint64_t idle;
  uint64_t busy;
};

const uint16_t REFLEX_MAGIC = 32;

// FIXME - these may be specific to our device
const uint64_t NUM_SECTORS = 3125627568;
const uint64_t LBA_ALIGNMENT = ~(0x7);
const uint64_t SECTOR_SIZE = 512;
const uint32_t LBA_COUNT = 8;

const uint16_t OPCODE_GET = 0x00;
const uint16_t OPCODE_SET = 0x01;

const uint16_t RESPONSE_NO_ERROR = 0x00;
const uint16_t RESPONSE_INV_ARG = 0x04;

std::vector<std::byte> writeReqData;

typedef struct __attribute__((__packed__)) {
  uint16_t magic;
  uint16_t opcode;
  uint64_t req_handle;
  unsigned long lba;
  unsigned int lba_count;
  uint64_t tsc;
} PacketHeader;

// The maximum lateness to tolerate before dropping egress samples.
constexpr uint64_t kMaxCatchUpUS = 5;

void PrintPacketHeader(const PacketHeader* p) {
  std::cout << "magic: " << p->magic
            << ", opcode: " << p->opcode
            << ", req_handle: " << p->req_handle
            << ", lba: " << p->lba
            << ", lba_count: " << p->lba_count
            << ", tsc: " << p->tsc << "\n";
}

struct work_unit {
  bool is_op_write;
  double start_us, work_us, duration_us;
  uint64_t tsc;
};

template <class Arrival>
std::vector<work_unit> GenerateWork(Arrival a, double cur_us, double last_us) {
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> rnd(0, 100);

  std::vector<work_unit> w;
  while (cur_us < last_us) {
    cur_us += a();
    const auto pct = rnd(rng);
    const bool is_op_write = pct <= write_pct;
    w.emplace_back(work_unit{is_op_write, cur_us, 0, 0});
  }
  return w;
}

std::vector<work_unit> ClientWorker(rt::TcpConn *c, rt::WaitGroup *starter,
    std::function<std::vector<work_unit>()> wf) {
  std::vector<work_unit> w(wf());
  std::vector<time_point<steady_clock>> timings;
  timings.reserve(w.size());

  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> rnd(0, NUM_SECTORS);

  // Start the receiver thread.
  auto th = rt::Thread([&] {
    PacketHeader rp;

    while (true) {
      // Read the packet header.
      ssize_t ret = c->ReadFull(&rp, sizeof(rp));
      if (ret != static_cast<ssize_t>(sizeof(rp))) {
        if (ret == 0 || ret < 0) break;
        panic("read failed, ret = %ld", ret);
      }

      // Verify the magic.
      // TODO girfan: This is probably a bad approach; should read magic first.
      // Only if magic matches should we read the rest of the payload.
      if (rp.magic != REFLEX_MAGIC) {
        panic("magic does not match, received magic = %d", rp.magic);
      }

      // PrintPacketHeader(&rp);

      if (rp.opcode == OPCODE_GET) {
        // Read the payload if this was a GET request.
        const size_t payload_size = rp.lba_count * SECTOR_SIZE;
        void* buf = malloc(payload_size);
        ssize_t ret = c->ReadFull(buf, payload_size);
        if (ret != static_cast<ssize_t>(payload_size)) {
          if (ret == 0 || ret < 0) break;
          panic("read failed, ret = %ld", ret);
        }
      }

      barrier();
      auto ts = steady_clock::now();
      barrier();
      uint64_t idx = rp.req_handle;
      w[idx].duration_us = duration_cast<sec>(ts - timings[idx]).count();
      w[idx].tsc = ntoh64(rp.tsc);
    }
  });

  // Synchronized start of load generation.
  starter->Done();
  starter->Wait();

  barrier();
  auto expstart = steady_clock::now();
  barrier();

  PacketHeader p;

  auto wsize = w.size();
  for (unsigned int i = 0; i < wsize; ++i) {
    uint64_t lba = (rnd(rng) % NUM_SECTORS) & LBA_ALIGNMENT;
    auto lba_count = LBA_COUNT;
    if (lba + LBA_COUNT > NUM_SECTORS) {
      lba_count = NUM_SECTORS - lba;
    }

    p.magic = REFLEX_MAGIC;
    p.opcode = w[i].is_op_write ? OPCODE_SET : OPCODE_GET;
    p.req_handle = static_cast<uint64_t>(i);
    p.lba = lba;
    p.lba_count = lba_count;
    p.tsc = 0;

    barrier();
    auto now = steady_clock::now();
    barrier();

    if (duration_cast<sec>(now - expstart).count() < w[i].start_us) {
      ssize_t ret = c->WriteFull(&p, sizeof(PacketHeader));
      if (ret != static_cast<ssize_t>(sizeof(PacketHeader)))
        panic("write failed, ret = %ld", ret);
      if (w[i].is_op_write) {
        ret = c->WriteFull(writeReqData.data(), lba_count * SECTOR_SIZE);
        if (ret != static_cast<ssize_t>(lba_count * SECTOR_SIZE))
          panic("write failed, ret = %ld", ret);
      }
      now = steady_clock::now();
      rt::Sleep(w[i].start_us - duration_cast<sec>(now - expstart).count());
    }
    if (duration_cast<sec>(now - expstart).count() - w[i].start_us >
        kMaxCatchUpUS)
      continue;

    barrier();
    timings[i] = steady_clock::now();
    barrier();
  }

  rt::Sleep(1 * rt::kSeconds);
  c->Shutdown(SHUT_RDWR);
  th.Join();

  return w;
}

std::vector<work_unit> RunExperiment(
    int threads, double *rps, std::function<std::vector<work_unit>()> wf) {
  // Create one TCP connection per thread.
  std::vector<std::unique_ptr<rt::TcpConn>> conns;
  const auto num_servers = servers.size();
  for (int i = 0; i < threads; ++i) {
    // Assign threads to servers in a round robin fashion.
    const netaddr raddr = servers[i % num_servers];
    std::unique_ptr<rt::TcpConn> outc(rt::TcpConn::Dial({0, 0}, raddr));
    if (unlikely(outc == nullptr)) panic("couldn't connect to raddr.");
    conns.emplace_back(std::move(outc));
  }

  // Launch a worker thread for each connection.
  rt::WaitGroup starter(threads + 1);
  std::vector<rt::Thread> th;
  std::unique_ptr<std::vector<work_unit>> samples[threads];
  for (int i = 0; i < threads; ++i) {
    th.emplace_back(rt::Thread([&, i] {
      auto v = ClientWorker(conns[i].get(), &starter, wf);
      samples[i].reset(new std::vector<work_unit>(std::move(v)));
    }));
  }

  // Give the workers time to initialize, then start recording.
  starter.Done();
  starter.Wait();

  // |--- start experiment duration timing ---|
  barrier();
  auto start = steady_clock::now();
  barrier();

  // Wait for the workers to finish.
  for (auto &t : th) t.Join();

  // |--- end experiment duration timing ---|
  barrier();
  auto finish = steady_clock::now();
  barrier();

  // Close the connections.
  for (auto &c : conns) c->Abort();

  // Aggregate all the samples together.
  std::vector<work_unit> w;
  for (int i = 0; i < threads; ++i) {
    auto &v = *samples[i];
    w.insert(w.end(), v.begin(), v.end());
  }

  // Remove requests that did not complete.
  w.erase(std::remove_if(w.begin(), w.end(),
                         [](const work_unit &s) { return s.duration_us == 0; }),
          w.end());

  double elapsed = duration_cast<sec>(finish - start).count();
  if (rps != nullptr)
    *rps = static_cast<double>(w.size()) / elapsed * 1000000;

  return w;
}

void PrintStatResults(std::vector<work_unit> w, double offered_rps,
                      double rps) {
  std::sort(w.begin(), w.end(), [](const work_unit &s1, work_unit &s2) {
    return s1.duration_us < s2.duration_us;
  });
  double sum = std::accumulate(
      w.begin(), w.end(), 0.0,
      [](double s, const work_unit &c) { return s + c.duration_us; });
  double mean = sum / w.size();
  double count = static_cast<double>(w.size());
  double p90 = w[count * 0.9].duration_us;
  double p99 = w[count * 0.99].duration_us;
  double p999 = w[count * 0.999].duration_us;
  double p9999 = w[count * 0.9999].duration_us;
  double min = w[0].duration_us;
  double max = w[w.size() - 1].duration_us;
  std::cout  //<<
             //"#threads,offered_rps,rps,samples,min,mean,p90,p99,p999,p9999,max"
             //<< std::endl
      << std::setprecision(4) << std::fixed << threads << "," << offered_rps
      << "," << rps << "," << "," << w.size() << "," << min << ","
      << mean << "," << p90 << "," << p99 << "," << p999 << "," << p9999 << ","
      << max << std::endl;
}

void SteadyStateExperiment(double offered_rps) {
  double rps;
  std::vector<work_unit> w = RunExperiment(threads, &rps, [=] {
    std::mt19937 rg(rand());
    std::mt19937 dg(rand());
    std::exponential_distribution<double> rd(
        1.0 / (1000000.0 / (offered_rps / static_cast<double>(threads))));
    return GenerateWork(std::bind(rd, rg), 0, 2000000);
  });

  PrintStatResults(w, offered_rps, rps);
}

void ClientHandler(void *arg) {
  const double step_size = max_rps / samples;
  for (double i = step_size; i <= max_rps; i += step_size) {
    SteadyStateExperiment(i);
  }
}

int StringToAddr(const char *str, uint32_t *addr) {
  uint8_t a, b, c, d;
  if (sscanf(str, "%hhu.%hhu.%hhu.%hhu", &a, &b, &c, &d) != 4) return -EINVAL;
  *addr = MAKE_IP_ADDR(a, b, c, d);
  return 0;
}

void FillWriteRequestData() {
  writeReqData.resize(LBA_COUNT * SECTOR_SIZE);
  for (uint64_t i = 0; i < writeReqData.size(); i++) {
    if (i % 2)
      writeReqData[i] = std::byte(0xa);
    else
      writeReqData[i] = std::byte(0xb);
  }
}

}  // anonymous namespace

int main(int argc, char *argv[]) {
  int ret;

  if (argc < 7) {
    std::cerr << "usage: [cfg_file] [nthreads] [mpps] [samples] [write_pct %] "
                 "[<ip> <ip> ...]\n";
    return -EINVAL;
  }

  threads = std::stoi(argv[2], nullptr, 0);
  max_rps = std::stod(argv[3]) * 1000000; // MPPS to RPS
  samples = std::stoi(argv[4]);
  write_pct = std::stod(argv[5]);

  for (int i = 6; i < argc; i++) {
    netaddr raddr;
    ret = StringToAddr(argv[i], &raddr.ip);
    if (ret) return -EINVAL;
    raddr.port = kNetbenchPort;
    servers.emplace_back(raddr);
  }

  FillWriteRequestData();

  ret = runtime_init(argv[1], ClientHandler, NULL);
  if (ret) {
    printf("failed to start runtime\n");
    return ret;
  }

  return 0;
}
