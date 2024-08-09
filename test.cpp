#include <bthread/bthread.h>
#include <bthread/task_group.h>
#include <bthread/unstable.h>
#include <butil/files/file_path.h>
#include <execinfo.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>

#include "csv_reader_bthread.h"
#include "parser.h"
// DEFINE_int32(bthread_concurrency, 8, "Number of bthread workers");

typedef Util::IO::LineReader<EchoCSVParser, Util::IO::SynchronousReader> Reader;
bthread::Mutex m;

char* __file_name;
int bthread_num;
int batch_size_log;
double iops, max_speed;
int use_pthread;

std::once_flag flag;

void* fun(void* arg) {
    // int fd = open(__file_name, O_RDONLY | O_DIRECT);
    // void* buf;
    // int sz = 1 << batch_size_log;
    // posix_memalign(&buf, 4096, sz);
    // while (read(fd, buf, sz)) {
    // }
    // printf("start %d\n", bthread_self());

    EchoCSVParser parser;
    uint64_t cnt = 0;
    std::string file_name = __file_name;
    int ss = 1 << batch_size_log;
    Reader reader(file_name, &parser, ss);
    while (reader.ReadLine() == 0) {
    }
    return nullptr;
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    google::SetLogDestination(google::GLOG_INFO, "./log/test.log.");
    __file_name = argv[1];
    bthread_num = atoi(argv[2]);
    batch_size_log = atoi(argv[3]);
    use_pthread = atoi(argv[4]);

    bthread_t tid[512];
    std::vector<std::thread> threads;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    for (int i = 1; i <= bthread_num; i++) {
        if (use_pthread) {
            threads.push_back(std::thread(fun, nullptr));
        } else {
            bthread_start_background(tid + i, &attr, fun, nullptr);
        }
    }
    if (use_pthread) {
        for (std::thread& t : threads) {
            t.join();
        }
    } else {
        for (int i = 1; i <= bthread_num; i++) {
            bthread_join(tid[i], nullptr);
        }
    }

    return 0;
}