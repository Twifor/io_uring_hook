#include <arrow/array.h>
#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <brpc/event_dispatcher.h>
#include <bthread/bthread.h>
#include <bthread/task_group.h>
#include <butil/files/file_path.h>
#include <execinfo.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <parquet/arrow/reader.h>
#include <signal.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <random>

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

std::vector<int> getRandomPermutation(int x) {
    std::vector<int> result;
    for (int i = 0; i < x; ++i) {
        result.push_back(i);
    }

    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::shuffle(result.begin(), result.end(),
                 std::default_random_engine(seed));
    return result;
}

void* fun(void* arg) {
    int id = reinterpret_cast<uint64_t>(arg);
    char* __file_name = new char[100];
    sprintf(__file_name, "/mnt/nvme/files/file%d.parquet", id);
    int fd = open(__file_name, O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open file" << std::endl;
        return nullptr;
    }
    std::shared_ptr<arrow::io::ReadableFile> infile =
        arrow::io::ReadableFile::Open(fd).ValueOrDie();
    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    arrow::Status status = parquet::arrow::OpenFile(
        infile, arrow::default_memory_pool(), &parquet_reader);
    // if (!status.ok()) {
    //     std::cerr << status.ToString() << std::endl;
    //     return nullptr;
    // }
    std::shared_ptr<parquet::FileMetaData> file_metadata =
        parquet_reader->parquet_reader()->metadata();
    int num_columns = file_metadata->num_columns();

    auto what = getRandomPermutation(num_columns);
    for (int col : what) {
        std::shared_ptr<arrow::ChunkedArray> column;
        arrow::Status status = parquet_reader->ReadColumn(col, &column);

        auto chunks = column->chunks();
        for (auto& chunk : chunks) {
            auto double_chunk =
                std::static_pointer_cast<arrow::DoubleArray>(chunk);
            for (int i = 0; i < double_chunk->length(); i++) {
                double ele = double_chunk->Value(i);
            }
        }

        if (!status.ok()) {
            std::cerr << "Failed to read column: " << status.ToString()
                      << std::endl;
            return nullptr;
        }
        column.reset();
        arrow::default_memory_pool()->ReleaseUnused();
    }

    return nullptr;
    // int id = reinterpret_cast<uint64_t>(arg);
    // EchoCSVParser parser;
    // uint64_t cnt = 0;
    // char* __file_name = new char[100];
    // sprintf(__file_name, "/mnt/nvme/files/file%d.csv", id);
    // std::string file_name = __file_name;
    // int sz = 1 << 16;
    // Reader reader(file_name, &parser, sz);
    // while (reader.ReadLine() == 0) {
    // }
    // // printf("%lld\n", parser.sum);
    // return nullptr;
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    // google::InitGoogleLogging(argv[0]);
    // google::SetLogDestination(google::GLOG_INFO, "./log/test.log.");
    bthread_num = atoi(argv[1]);
    batch_size_log = atoi(argv[2]);
    use_pthread = atoi(argv[3]);

    brpc::EventDispatcher& dispatcher = brpc::GetGlobalEventDispatcher(0);

    bthread_t tid[2048];
    std::vector<std::thread> threads;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    for (int i = 1; i <= bthread_num; i++) {
        if (use_pthread) {
            threads.push_back(std::thread(fun, (void*)i));
        } else {
            bthread_start_background(tid + i, &attr, fun, (void*)i);
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