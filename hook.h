#pragma once

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

struct IOTask;
struct Statistic;
inline void submit();
inline IOTask* is_submit_complete(IOTask* old_head);
inline bool add_to_sq_no_wait(IOTask* task);
inline void waitfor_epoll(IOTask* task);
inline void loop_add_to_sq(IOTask* task, bool enable_wait = false);
static void* keep_submit(void* arg);
inline void start_keep_submit(IOTask* tail);
inline void add_to_mpsc(IOTask* task);
inline bool is_brpc_co_environment();
static void lock_fn_init();
static void __on_io_uring_epoll_handler(int events);

extern "C" int open(const char* __path, int __oflag, ...);

extern "C" int close(int __fd);

extern "C" ssize_t read(int __fd, void* __buf, size_t __nbytes);

extern "C" ssize_t write(int __fd, const void* __buf, size_t __n);

// set exec-wrapper env 'LD_PRELOAD=./build/liburinghook.so'