#include <brpc/event_dispatcher.h>
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <butil/logging.h>
#include <butil/object_pool.h>
#include <butil/scoped_lock.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <liburing.h>
#include <sys/stat.h>

#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <memory>
#define ____cacheline_aligned __attribute__((__aligned__(64)))
#define EXPORT __attribute__((visibility("default")))

/*
 * env variables:
 * IOURING_HOOK_LOG_FREQ = 100
 * IOURING_HOOK_ENABLE_SQPOLL = false
 * IOURING_HOOK_INSTANCES = 4
 * IOURING_HOOK_QUEUE_DEPTH = 256
 */

static int IOURING_HOOK_INSTANCES;    // number of instances
static int IOURING_HOOK_QUEUE_DEPTH;  // queue size, should be 2^m
static const int FD_SIZE = 8192000;   // maximum file descriptors size
static int IOURING_HOOK_LOG_FREQ;
static bool IOURING_HOOK_ENABLE_SQPOLL;
static std::atomic<int> ref_counter;

enum IOType { READ, PREAD };
class IOTask;
class IOUring;
inline void submit(int io_uring_id);
inline bool is_submit_complete(IOTask* old_head, bool is_cur_task_resolved);
inline void loop_add_to_sq(IOTask* old_head, bool enable_wait);
inline void add_to_mpsc(IOTask* task);
inline bool add_to_sq(IOTask* task, bool enable_wait);
static void* keep_submit(void* arg);
inline void start_keep_submit(IOTask* tail);
inline bool is_brpc_co_environment();
static void lock_fn_init();
static void on_io_uring_epoll_handler(int events);
static __attribute__((constructor)) void library_init();
static __attribute__((destructor)) void library_cleanup();

typedef int (*fn_open_ptr)(const char*, int, ...);
typedef int (*fn_close_ptr)(int);
typedef ssize_t (*fn_read_ptr)(int __fd, void* __buf, size_t __nbytes);
typedef int (*fn_openat_ptr)(int __fd, const char* __path, int __oflag, ...);
typedef ssize_t (*fn_pread_ptr)(int __fd, void* __buf, size_t __nbytes,
                                __off64_t __offset);

static bool is_file_fd[FD_SIZE];    // is a file? (not socket)
static ssize_t fd_offset[FD_SIZE];  // offset of files
static ssize_t fd_size[FD_SIZE];    // file size
static int fd_iouring[FD_SIZE];     // id of iouring instances
static IOUring* io_uring_instances;

static fn_open_ptr fn_open;
static fn_close_ptr fn_close;
static fn_read_ptr fn_read;
static fn_openat_ptr fn_openat;
static fn_pread_ptr fn_pread;

extern "C" int open(const char* __path, int __oflag, ...) EXPORT;
extern "C" int close(int __fd) EXPORT;
extern "C" ssize_t read(int __fd, void* __buf, size_t __nbytes) EXPORT;
extern "C" int openat(int __fd, const char* __path, int __oflag, ...) EXPORT;
extern "C" ssize_t pread(int __fd, void* __buf, size_t __nbytes,
                         __off64_t __offset) EXPORT;
extern "C" ssize_t pread64(int __fd, void* __buf, size_t __nbytes,
                           __off64_t __offset) EXPORT;

struct Statistic {
    uint64_t sum = 0;
    uint64_t sum2 = 0;
    uint64_t cnt = 0;
    int id;
    inline void add(int x) {
        sum += x;
        sum2 += 1LL * x * x;
        ++cnt;
        if (cnt == IOURING_HOOK_LOG_FREQ) {
            show();
            sum = sum2 = cnt = 0;
        }
    }
    inline void show() const {
        LOG(INFO) << "Submit Info [" << id << "]: Mean=" << mean()
                  << ", Std=" << std() << ", Ref count: " << ref_counter.load();
    }
    inline double mean() const { return 1.0 * sum / cnt; }
    inline double std() const {
        return sqrt(1.0 * sum2 / cnt - mean() * mean());
    }
};

struct ____cacheline_aligned IOTask {
   private:
    butil::atomic<int> cnt;
    ssize_t return_value;
    // destroy instance and return object
    inline void release() {
        ref_counter.fetch_sub(1);
        assert(cnt.load() == 0);
        next = nullptr;
        bthread::butex_destroy(butex);
        butil::return_object(this);
    }

   public:
    // count + 1
    inline void ref() { cnt.fetch_add(1, butil::memory_order_relaxed); }
    // count - 1
    inline void unref() {
        int res = cnt.fetch_sub(1, butil::memory_order_relaxed);
        if (res == 1) release();
    }

    inline void set_return_value(ssize_t res) {
        if (res < 0 && res != -4 && res != -11) {
            LOG(FATAL) << "Invalid result: " << res << " " << strerror(-res);
            exit(-1);
        }
        return_value = res;
        // bug fix (for cephFS)
        if (type == IOType::READ &&
            return_value + fd_offset[fd] > fd_size[fd]) {
            if (fd_offset[fd] > fd_size[fd]) {
                LOG(FATAL) << "Internal Error: Invalid offset " << fd_offset[fd]
                           << " with file size " << fd_size[fd];
                exit(-1);
            }
            return_value = fd_size[fd] - fd_offset[fd];
        } else if (type == IOType::PREAD &&
                   return_value + offset > fd_size[fd]) {
            return_value = fd_size[fd] - offset;
        }
    }

    inline ssize_t get_return_value() const { return return_value; }

    // wait for processing (blocking)
    void wait_process() {
        const int expected_val = butex->load(butil::memory_order_acquire);
        add_to_mpsc(this);
        int rc = bthread::butex_wait(butex, expected_val, nullptr);
        if (rc < 0 && errno != 11) {
            LOG(FATAL) << "butex error: " << strerror(errno);
        }
    }
    // wake up the corresponding blocked bthread (in wait_process)
    void notify() {
        butex->fetch_add(1, butil::memory_order_release);
        bthread::butex_wake(butex);
    }
    // initialize IOTask instance
    // you should call this function after get_object()
    IOTask* init() {
        ref_counter.fetch_add(1);
        butex = bthread::butex_create_checked<butil::atomic<int>>();
        next = nullptr;
        cnt.store(0, butil::memory_order_release);
        return this;
    }
    void* buf;
    IOType type;
    int fd;
    ssize_t nbytes;
    off64_t offset;
    butil::atomic<int>* butex;
    IOTask* next;
};

struct IOUring {
    struct io_uring ring;              // iouring instance
    butil::atomic<IOTask*> mpsc_head;  // the head of mpsc queue
    bthread_t keep_submit_bthread;     // the bthread for submitting
    butil::atomic<int>* epoll_butex;   // the butex for epoll out
    Statistic statistic;
    IOUring() { mpsc_head.store(nullptr); }
    void init(brpc::EventDispatcher& dispatcher, int io_uring_id) {
        io_uring_queue_init(
            IOURING_HOOK_QUEUE_DEPTH, &ring,
            IOURING_HOOK_ENABLE_SQPOLL ? IORING_SETUP_SQPOLL : 0);
        epoll_butex = bthread::butex_create_checked<butil::atomic<int>>();
        if (dispatcher.AddIOuringEpoll(ring.ring_fd, io_uring_id)) {
            LOG(FATAL) << "Cannot add io_uring epoll instance.";
        }
        statistic.id = io_uring_id;
    }
    void destroy(brpc::EventDispatcher& dispatcher) {
        dispatcher.RemoveIOuringEpoll(ring.ring_fd);
        io_uring_queue_exit(&ring);
        bthread::butex_destroy(epoll_butex);
    }
};

inline void fd_init(int fd) {
    struct stat statbuf;
    if (fstat(fd, &statbuf) == -1) {
        perror("fstat");
        return;
    }
    if (S_ISREG(statbuf.st_mode) && is_brpc_co_environment()) {
        is_file_fd[fd] = true;
        fd_offset[fd] = 0;
        fd_size[fd] = statbuf.st_size;
        fd_iouring[fd] = fd % IOURING_HOOK_INSTANCES;
    }
}

// add one task to mpsc queue
inline void add_to_mpsc(IOTask* task) {
    task->ref();
    IOTask* prev_head =
        io_uring_instances[fd_iouring[task->fd]].mpsc_head.exchange(
            task, butil::memory_order_release);
    if (prev_head != nullptr) {
        task->next = prev_head;
        return;
    }
    // We've got the right to submit.
    task->next = nullptr;
    loop_add_to_sq(task, false);
}

// submit all sqe
inline void submit(int io_uring_id) {
retry:
    int ret = io_uring_submit(&io_uring_instances[io_uring_id].ring);
    if (ret < 0) {
        if (-ret == 11 || -ret == 16) {  // EAGAIN or EBUSY
            bthread_usleep(10);
            goto retry;
        }
        LOG(FATAL) << "io_uring_submit: " << strerror(-ret);
    }
    io_uring_instances[io_uring_id].statistic.add(ret);
}

// whether the new head of mspc is old_head?
// if both is_cur_task_resolved = true and the new head is old_head,
// the lock will be released
inline bool is_submit_complete(IOTask* old_head, bool is_cur_task_resolved) {
    IOTask *desired = nullptr, *new_head = old_head;
    if (!is_cur_task_resolved) {
        desired = old_head;
    }
    // attempt to do CAS
    if (io_uring_instances[fd_iouring[old_head->fd]]
            .mpsc_head.compare_exchange_strong(new_head, desired,
                                               butil::memory_order_acquire)) {
        return true;
    }
    IOTask *current_task = new_head, *last_task = nullptr;
    do {
        while (current_task->next == nullptr) {  // broken link, we should wait
            sched_yield();
        }
        IOTask* cached_next = current_task->next;
        current_task->next = last_task;
        last_task = current_task;
        current_task = cached_next;
    } while (current_task != old_head);
    old_head->next = last_task;
    return false;
}

// add tasks to sq, from old_head
// if enable_wait = true, we will wait for epoll_out
inline void loop_add_to_sq(IOTask* old_head, bool enable_wait) {
resubmit:
    IOTask *current_task = old_head, *last_task = nullptr;
    do {
        while (current_task) {
            if (last_task) {
                last_task->unref();
            }
            if (!add_to_sq(current_task, enable_wait)) {
                return;
            }
            last_task = current_task;
            current_task = current_task->next;
        }
        if (is_submit_complete(last_task, false)) {
            submit(fd_iouring[old_head->fd]);
            break;
        }
        current_task = last_task->next;
    } while (1);
    if (!is_submit_complete(last_task, true)) {
        IOTask* cached_last_task_next = last_task->next;
        last_task->unref();
        if (enable_wait) {
            old_head = cached_last_task_next;
            goto resubmit;
        } else {
            start_keep_submit(cached_last_task_next);
        }
    } else {
        last_task->unref();
    }
}

// add one task to sq
// if enable_wait = true, we will wait for epoll_out
inline bool add_to_sq(IOTask* task, bool enable_wait) {
    struct io_uring_sqe* sqe;
    const int expected_val =
        io_uring_instances[fd_iouring[task->fd]].epoll_butex->load(
            butil::memory_order_acquire);
    sqe = io_uring_get_sqe(&io_uring_instances[fd_iouring[task->fd]].ring);
    if (!sqe) {  // there is no empty sqe
        if (enable_wait) {
            submit(fd_iouring[task->fd]);  // submit and wait for epoll_out
            int rc = bthread::butex_wait(
                io_uring_instances[fd_iouring[task->fd]].epoll_butex,
                expected_val, nullptr);
            if (rc < 0 && errno != 11) {  // EAGAIN
                LOG(FATAL) << "butex error: " << strerror(errno);
            }
            sqe = io_uring_get_sqe(
                &io_uring_instances[fd_iouring[task->fd]].ring);
            assert(sqe);
        } else {
            start_keep_submit(task);  // start another bthread to submit tasks
            return false;
        }
    }
    sqe->user_data = reinterpret_cast<long long>(task);  // record task
    if (task->type == IOType::READ) {                    // read
        io_uring_prep_read(sqe, task->fd, task->buf, task->nbytes,
                           fd_offset[task->fd]);
    } else if (task->type == IOType::PREAD) {
        io_uring_prep_read(
            sqe, task->fd, task->buf, task->nbytes,
            task->offset);  // the offset of this task, not the global one
    }
    task->ref();
    return true;
}

static void* keep_submit(void* arg) {
    // printf("keep submit from %p\n", tail.get());
    // in bthread, keep submitting requests
    loop_add_to_sq(static_cast<IOTask*>(arg), true);
    return nullptr;
}

// start a bthread to submit tasks (from tail)
inline void start_keep_submit(IOTask* tail) {
    // resolve errno
    bthread_start_background(
        &io_uring_instances[fd_iouring[tail->fd]].keep_submit_bthread, nullptr,
        keep_submit, tail);
}

inline bool is_brpc_co_environment() { return bthread_self() != 0; }

static void lock_fn_init() {
    static std::atomic<bool> fn_init_lock;
    static std::once_flag fn_init_flag;
    std::call_once(fn_init_flag, [&]() {
        fn_open = reinterpret_cast<fn_open_ptr>(dlsym(RTLD_NEXT, "open"));
        fn_close = reinterpret_cast<fn_close_ptr>(dlsym(RTLD_NEXT, "close"));
        fn_read = reinterpret_cast<fn_read_ptr>(dlsym(RTLD_NEXT, "read"));
        fn_openat = reinterpret_cast<fn_openat_ptr>(dlsym(RTLD_NEXT, "openat"));
        fn_pread = reinterpret_cast<fn_pread_ptr>(dlsym(RTLD_NEXT, "pread"));
        if (!fn_open || !fn_close || !fn_read || !fn_openat || !fn_pread) {
            LOG(FATAL) << "Cannot extract systemcalls from dynamic libs.";
            exit(EXIT_FAILURE);
        }
        fn_init_lock.store(true);
    });
    while (fn_init_lock.load() == false);
}

static void on_io_uring_epoll_handler(int events, int io_uring_id) {
    if (io_uring_id < 0 || io_uring_id >= IOURING_HOOK_INSTANCES) {
        LOG(FATAL) << "io_uring_id = " << io_uring_id
                   << ", but the number of instances = "
                   << IOURING_HOOK_INSTANCES;
        exit(-1);
    }
    if (events & EPOLLOUT) {
        io_uring_instances[io_uring_id].epoll_butex->fetch_add(
            1, butil::memory_order_release);
        bthread::butex_wake(io_uring_instances[io_uring_id].epoll_butex);
    }
    if (events & EPOLLIN) {
        io_uring_cqe* cqe;
        do {
            unsigned head, nr = 0;
            io_uring_for_each_cqe(&io_uring_instances[io_uring_id].ring, head,
                                  cqe) {
                ++nr;
                IOTask* task = reinterpret_cast<IOTask*>(cqe->user_data);
                task->set_return_value(cqe->res);
                task->notify();
                task->unref();
            }
            io_uring_cq_advance(&io_uring_instances[io_uring_id].ring, nr);
        } while (io_uring_peek_cqe(&io_uring_instances[io_uring_id].ring,
                                   &cqe) == 0);
    }
}

static __attribute__((constructor)) void library_init() {
    char* env_pointer = std::getenv("IOURING_HOOK_LOG_FREQ");
    IOURING_HOOK_LOG_FREQ = env_pointer != nullptr ? atoi(env_pointer) : 10000;
    env_pointer = std::getenv("IOURING_HOOK_ENABLE_SQPOLL");
    IOURING_HOOK_ENABLE_SQPOLL =
        env_pointer != nullptr && strcmp(env_pointer, "true") == 0;
    env_pointer = std::getenv("IOURING_HOOK_INSTANCES");
    IOURING_HOOK_INSTANCES = env_pointer != nullptr ? atoi(env_pointer) : 4;
    env_pointer = std::getenv("IOURING_HOOK_QUEUE_DEPTH");
    IOURING_HOOK_QUEUE_DEPTH = env_pointer != nullptr ? atoi(env_pointer) : 256;

    io_uring_instances = new IOUring[IOURING_HOOK_INSTANCES];
    brpc::EventDispatcher& dispatcher = brpc::GetGlobalEventDispatcher(0);
    dispatcher.RegisterIOuringHandler(on_io_uring_epoll_handler);
    for (int i = 0; i < IOURING_HOOK_INSTANCES; i++) {
        io_uring_instances[i].init(dispatcher, i);
    }
    if (!dispatcher.Running()) {
        brpc::GetGlobalEventDispatcher(0).Start(nullptr);
    }
    LOG(INFO) << "IOUring Hook successfully.";
    LOG(INFO) << "log freq = " << IOURING_HOOK_LOG_FREQ;
    LOG(INFO) << "sqpoll = " << IOURING_HOOK_ENABLE_SQPOLL;
    LOG(INFO) << "number of io_uring instances = " << IOURING_HOOK_INSTANCES;
    LOG(INFO) << "queue depth = " << IOURING_HOOK_QUEUE_DEPTH;
}

static __attribute__((destructor)) void library_cleanup() {
    brpc::EventDispatcher& dispatcher = brpc::GetGlobalEventDispatcher(0);
    dispatcher.Stop();
    dispatcher.Join();
    for (int i = 0; i < IOURING_HOOK_INSTANCES; i++) {
        io_uring_instances[i].destroy(dispatcher);
    }
    delete[] io_uring_instances;
    LOG(INFO) << "Hook Finished";
}

extern "C" int open(const char* __path, int __oflag, ...) {
    if (fn_open == nullptr) {
        lock_fn_init();
    }
    va_list args;
    va_start(args, __oflag);
    int fd;
    if (__oflag & O_CREAT) {
        fd = fn_open(__path, __oflag, va_arg(args, mode_t));
    } else {
        fd = fn_open(__path, __oflag);
    }
    va_end(args);
    fd_init(fd);
    return fd;
}

extern "C" int openat(int __fd, const char* __path, int __oflag, ...) {
    if (fn_openat == nullptr) {
        lock_fn_init();
    }
    va_list args;
    va_start(args, __oflag);
    int fd;
    if (__oflag & O_CREAT) {
        fd = fn_openat(__fd, __path, __oflag, va_arg(args, mode_t));
    } else {
        fd = fn_openat(__fd, __path, __oflag);
    }
    va_end(args);
    fd_init(fd);
    return fd;
}

extern "C" int close(int __fd) {
    if (fn_close == nullptr) {
        lock_fn_init();
    }
    is_file_fd[__fd] = false;
    return fn_close(__fd);
}

extern "C" ssize_t read(int __fd, void* __buf, size_t __nbytes) {
    if (fn_read == nullptr) {
        lock_fn_init();
    }
    if (is_file_fd[__fd] && is_brpc_co_environment()) {
        ssize_t ret;
        do {
            IOTask* task = butil::get_object<IOTask>()->init();
            task->ref();
            task->type = IOType::READ, task->fd = __fd;
            task->buf = __buf, task->nbytes = __nbytes;
            task->wait_process();
            ret = task->get_return_value();
            task->unref();
        } while (ret == -4 || ret == -11);
        fd_offset[__fd] += ret;
        return ret;
    } else {
        ssize_t __bytes = fn_read(__fd, __buf, __nbytes);
        fd_offset[__fd] += __bytes;
        return __bytes;
    }
}

extern "C" ssize_t pread(int __fd, void* __buf, size_t __nbytes,
                         __off64_t __offset) {
    if (fn_read == nullptr) {
        lock_fn_init();
    }
    if (is_file_fd[__fd] && is_brpc_co_environment()) {
        ssize_t ret;
        do {
            IOTask* task = butil::get_object<IOTask>()->init();
            task->ref();
            task->type = IOType::PREAD, task->fd = __fd;
            task->buf = __buf, task->nbytes = __nbytes;
            task->offset = __offset;
            task->wait_process();
            ret = task->get_return_value();
            task->unref();
        } while (ret == -4 || ret == -11);
        return ret;
    } else {
        ssize_t __bytes = fn_pread(__fd, __buf, __nbytes, __offset);
        return __bytes;
    }
}

extern "C" ssize_t pread64(int __fd, void* __buf, size_t __nbytes,
                           __off64_t __offset) {
    return pread(__fd, __buf, __nbytes, __offset);
}