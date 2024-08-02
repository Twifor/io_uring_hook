#include <atomic>
#include <brpc/event_dispatcher.h>
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <butil/logging.h>
#include <butil/object_pool.h>
#include <butil/scoped_lock.h>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <dlfcn.h>
#include <fcntl.h>
#include <liburing.h>
#include <memory>
#include <sys/stat.h>

enum IOType { READ, WRITE };
const int QD = 4;
const int FD_SIZE = 8192000;
#define ____cacheline_aligned __attribute__((__aligned__(64)))

struct Statistic {
    uint64_t sum = 0;
    uint64_t sum2 = 0;
    uint64_t cnt = 0;
    inline void add(int x);
    inline double mean() const;
    inline double std() const;
};

struct ____cacheline_aligned IOTask {
private:
    butil::atomic<int> cnt;
    inline void release();

public:
    inline void ref();
    inline void unref();

    // class __IOTaskPtr {
    // private:
    //     IOTask* ptr;

    // public:
    //     IOTask* get() const;
    //     __IOTaskPtr(IOTask* p = nullptr);
    //     __IOTaskPtr(const __IOTaskPtr& p);
    //     const __IOTaskPtr& operator=(const __IOTaskPtr& p);
    //     __IOTaskPtr(__IOTaskPtr&& p) = delete;
    //     ~__IOTaskPtr();
    //     IOTask* operator->();
    // };
    void* buf;
    IOType type;
    int fd;
    ssize_t nbytes;
    ssize_t return_value;
    butil::atomic<int>* butex;
    IOTask* next;
    void wait_process();
    IOTask* init();
    void notify();
};

// typedef IOTask::__IOTaskPtr IOTaskPtr;
static Statistic statistic;
static bool is_file_fd[FD_SIZE];     // is a file? (not socket)
static uint64_t fd_offset[FD_SIZE];  // offset of files
static struct io_uring ring;
static butil::atomic<IOTask*> mpsc_head;
static bthread_t keep_submit_bthread;
static butil::atomic<int>* epoll_butex;

// original functions
typedef int (*fn_open_ptr)(const char*, int, ...);
typedef int (*fn_close_ptr)(int);
typedef ssize_t (*fn_read_ptr)(int __fd, void* __buf, size_t __nbytes);
typedef ssize_t (*fn_write_ptr)(int __fd, const void* __buf, size_t __n);
typedef int (*fn_openat_ptr)(int __fd, const char* __path, int __oflag, ...);
static fn_open_ptr fn_open = nullptr;
static fn_close_ptr fn_close = nullptr;
static fn_read_ptr fn_read = nullptr;
static fn_write_ptr fn_write = nullptr;
static fn_openat_ptr fn_openat = nullptr;
inline void submit();
inline IOTask* is_submit_complete(IOTask* old_head, IOTask*& new_tail);
inline void loop_add_to_sq(IOTask* task, bool enable_wait = false);
inline void add_to_mpsc(IOTask* task);
inline bool add_to_sq_no_wait(IOTask* task);
inline void waitfor_epoll(IOTask* task);
static void* keep_submit(void* arg);
inline void start_keep_submit(IOTask* tail);
inline bool is_brpc_co_environment();
static void lock_fn_init();
static void on_io_uring_epoll_handler(int events);
__attribute__((constructor)) void library_init();
__attribute__((destructor)) void library_cleanup();
extern "C" int open(const char* __path, int __oflag, ...);
extern "C" int close(int __fd);
extern "C" ssize_t read(int __fd, void* __buf, size_t __nbytes);
extern "C" ssize_t write(int __fd, const void* __buf, size_t __nbytes);

// set exec-wrapper env 'LD_PRELOAD=./build/liburinghook.so'