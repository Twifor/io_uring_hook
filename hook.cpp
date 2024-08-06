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

/*
 * env variables:
 * IOURING_HOOK_LOG_FREQ = 100
 * IOURING_HOOK_SQPOLL = false
 */

int QD = 256;                 // queue size, should be 2^m
const int FD_SIZE = 8192000;  // maximum file descriptors size
int LOG_FREQ;
bool ENABLE_SQPOLL;

enum IOType { READ, WRITE };
class IOTask;
inline void submit();
inline bool is_submit_complete(IOTask* old_head, bool is_cur_task_resolved);
inline void loop_add_to_sq(IOTask* old_head, bool enable_wait);
inline void add_to_mpsc(IOTask* task);
inline bool add_to_sq(IOTask* task, bool enable_wait);
static void* keep_submit(void* arg);
inline void start_keep_submit(IOTask* tail);
inline bool is_brpc_co_environment();
static void lock_fn_init();
static void on_io_uring_epoll_handler(int events);
typedef int (*fn_open_ptr)(const char*, int, ...);
typedef int (*fn_close_ptr)(int);
typedef ssize_t (*fn_read_ptr)(int __fd, void* __buf, size_t __nbytes);
typedef ssize_t (*fn_write_ptr)(int __fd, const void* __buf, size_t __n);
typedef int (*fn_openat_ptr)(int __fd, const char* __path, int __oflag, ...);
__attribute__((constructor)) void library_init();
__attribute__((destructor)) void library_cleanup();
extern "C" int open(const char* __path, int __oflag, ...);
extern "C" int close(int __fd);
extern "C" ssize_t read(int __fd, void* __buf, size_t __nbytes);
extern "C" ssize_t write(int __fd, const void* __buf, size_t __nbytes);

struct Statistic {
    uint64_t sum = 0;
    uint64_t sum2 = 0;
    uint64_t cnt = 0;
    inline void add(int x) {
        sum += x;
        sum2 += 1LL * x * x;
        ++cnt;
        if (cnt == LOG_FREQ) {
            show();
            sum = sum2 = cnt = 0;
        }
    }
    inline void show() const {
        LOG(DEBUG) << "Submit Info: Mean=" << mean() << ", Std=" << std();
    }
    inline double mean() const { return 1.0 * sum / cnt; }
    inline double std() const {
        return sqrt(1.0 * sum2 / cnt - mean() * mean());
    }
};

struct ____cacheline_aligned IOTask {
   private:
    butil::atomic<int> cnt;
    // destroy instance and return object
    inline void release() {
        assert(cnt.load() == 0);
        next = nullptr;
        bthread::butex_destroy(butex);
        // printf("return back %p\n", this);
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

    // wait for processing (blocking)
    void wait_process() {
        const int expected_val = butex->load(butil::memory_order_acquire);
        add_to_mpsc(this);
        // assert(this->flag.load());
        // printf("wait %d %p\n", bthread_self(), this);
        int rc = bthread::butex_wait(butex, expected_val, nullptr);
        if (rc < 0 && errno != 11) {
            LOG(FATAL) << "butex error: " << strerror(errno);
        }
    }
    // wake up the corresponding blocked bthread (in wait_process)
    void notify() {
        butex->fetch_add(1, butil::memory_order_release);
        // printf("notify %p\n", this);
        bthread::butex_wake(butex);
    }
    // initialize IOTask instance
    // you should call this function after get_object()
    IOTask* init() {
        butex = bthread::butex_create_checked<butil::atomic<int>>();
        next = nullptr;
        cnt.store(0, butil::memory_order_release);
        return this;
    }
    void* buf;
    IOType type;
    int fd;
    ssize_t nbytes;
    ssize_t return_value;
    butil::atomic<int>* butex;
    IOTask* next;
};

static Statistic statistic;
static bool is_file_fd[FD_SIZE];          // is a file? (not socket)
static uint64_t fd_offset[FD_SIZE];       // offset of files
static struct io_uring ring;              // iouring instance
static butil::atomic<IOTask*> mpsc_head;  // the head of mpsc queue
static bthread_t keep_submit_bthread;     // the bthread for submitting
static butil::atomic<int>* epoll_butex;   // the butex for epoll out

static fn_open_ptr fn_open = nullptr;
static fn_close_ptr fn_close = nullptr;
static fn_read_ptr fn_read = nullptr;
static fn_write_ptr fn_write = nullptr;
static fn_openat_ptr fn_openat = nullptr;

// add one task to mpsc queue
inline void add_to_mpsc(IOTask* task) {
    task->ref();
    IOTask* prev_head = mpsc_head.exchange(task, butil::memory_order_release);
    // printf("link %p->%p\n", task.get(), prev_head.get());
    if (prev_head != nullptr) {
        task->next = prev_head;
        return;
    }
    // We've got the right to submit.
    task->next = nullptr;
    loop_add_to_sq(task, false);
}

// submit all sqe
inline void submit() {
retry:
    int ret = io_uring_submit(&ring);
    if (ret < 0) {
        if (-ret == 11 || -ret == 16) {  // EAGAIN or EBUSY
            bthread_usleep(10);
            goto retry;
        }
        LOG(FATAL) << "io_uring_submit: " << strerror(-ret);
    }
    statistic.add(ret);
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
    if (mpsc_head.compare_exchange_strong(new_head, desired,
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
            submit();
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
    const int expected_val = epoll_butex->load(butil::memory_order_acquire);
    sqe = io_uring_get_sqe(&ring);
    if (!sqe) {  // there is no empty sqe
        if (enable_wait) {
            submit();  // submit and wait for epoll_out
            int rc = bthread::butex_wait(epoll_butex, expected_val, nullptr);
            if (rc < 0 && errno != 11) {  // EAGAIN
                LOG(FATAL) << "butex error: " << strerror(errno);
            }
            sqe = io_uring_get_sqe(&ring);
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
    } else if (task->type == IOType::WRITE) {  // write
        io_uring_prep_write(sqe, task->fd, task->buf, task->nbytes,
                            fd_offset[task->fd]);
    }
    task->ref();
    // printf("sqe %p\n", task.get());
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
    // resovle errno
    bthread_start_background(&keep_submit_bthread, nullptr, keep_submit, tail);
}

inline bool is_brpc_co_environment() { return bthread_self() != 0; }

static void lock_fn_init() {
    static std::atomic<bool> fn_init_lock;
    static std::once_flag fn_init_flag;
    std::call_once(fn_init_flag, [&]() {
        fn_open = reinterpret_cast<fn_open_ptr>(dlsym(RTLD_NEXT, "open"));
        fn_close = reinterpret_cast<fn_close_ptr>(dlsym(RTLD_NEXT, "close"));
        fn_read = reinterpret_cast<fn_read_ptr>(dlsym(RTLD_NEXT, "read"));
        fn_write = reinterpret_cast<fn_write_ptr>(dlsym(RTLD_NEXT, "write"));
        fn_openat = reinterpret_cast<fn_openat_ptr>(dlsym(RTLD_NEXT, "openat"));
        if (!fn_open || !fn_close || !fn_read || !fn_write || !fn_openat) {
            LOG(FATAL) << "Cannot extract systemcalls from dynamic libs.";
            exit(EXIT_FAILURE);
        }
        fn_init_lock.store(true);
    });
    while (fn_init_lock.load() == false);
}

static void on_io_uring_epoll_handler(int events) {
    if (events & EPOLLOUT) {
        epoll_butex->fetch_add(1, butil::memory_order_release);
        bthread::butex_wake(epoll_butex);
    }
    if (events & EPOLLIN) {
        io_uring_cqe* cqe;
        do {
            unsigned head, nr = 0;
            io_uring_for_each_cqe(&ring, head, cqe) {
                ++nr;
                IOTask* task = reinterpret_cast<IOTask*>(cqe->user_data);
                task->return_value = cqe->res;
                task->notify();
                task->unref();
            }
            io_uring_cq_advance(&ring, nr);
        } while (io_uring_peek_cqe(&ring, &cqe) == 0);
    }
}

__attribute__((constructor)) void library_init() {
    char* env_pointer = std::getenv("IOURING_HOOK_LOG_FREQ");
    LOG_FREQ = env_pointer != nullptr ? atoi(env_pointer) : 10000;
    env_pointer = std::getenv("IOURING_HOOK_SQPOLL");
    ENABLE_SQPOLL = env_pointer != nullptr && strcmp(env_pointer, "true") == 0;

    io_uring_queue_init(QD, &ring, ENABLE_SQPOLL ? IORING_SETUP_SQPOLL : 0);
    epoll_butex = bthread::butex_create_checked<butil::atomic<int>>();
    brpc::EventDispatcher& dispatcher =
        brpc::GetGlobalEventDispatcher(ring.ring_fd);
    dispatcher.RegisterIOuringHandler(on_io_uring_epoll_handler);
    if (dispatcher.AddIOuringEpoll(ring.ring_fd)) {
        LOG(FATAL) << "Cannot add io_uring epoll instance.";
    }
    if (!dispatcher.Running()) {
        brpc::GetGlobalEventDispatcher(ring.ring_fd).Start(nullptr);
    }
    LOG(INFO) << "IOUring Hook successfully.";
    LOG(INFO) << "log freq = " << LOG_FREQ;
    LOG(INFO) << "sqpoll = " << ENABLE_SQPOLL;
}

__attribute__((destructor)) void library_cleanup() {
    io_uring_queue_exit(&ring);
    bthread::butex_destroy(epoll_butex);
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
    struct stat statbuf;
    if (fstat(fd, &statbuf) == -1) {
        perror("fstat");
        return fd;
    }
    if (S_ISREG(statbuf.st_mode) && is_brpc_co_environment()) {
        is_file_fd[fd] = true;
        fd_offset[fd] = 0;
    }
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
        IOTask* task = butil::get_object<IOTask>()->init();
        task->ref();
        task->type = IOType::READ, task->fd = __fd;
        task->buf = __buf, task->nbytes = __nbytes;
        task->wait_process();
        fd_offset[__fd] += task->return_value;
        ssize_t ret = task->return_value;
        task->unref();
        return ret;
    } else {
        ssize_t __bytes = fn_read(__fd, __buf, __nbytes);
        fd_offset[__fd] += __bytes;
        return __bytes;
    }
}

extern "C" ssize_t write(int __fd, const void* __buf, size_t __nbytes) {
    if (fn_write == nullptr) {
        lock_fn_init();
    }
    if (is_file_fd[__fd] && is_brpc_co_environment()) {
        IOTask* task = butil::get_object<IOTask>()->init();
        task->ref();
        task->type = IOType::WRITE, task->fd = __fd;
        task->buf = const_cast<void*>(__buf), task->nbytes = __nbytes;
        task->wait_process();
        fd_offset[__fd] += task->return_value;
        ssize_t ret = task->return_value;
        task->unref();
        return ret;
    } else {
        ssize_t __bytes = fn_write(__fd, __buf, __nbytes);
        fd_offset[__fd] += __bytes;
        return __bytes;
    }
}