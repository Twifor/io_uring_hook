#include "hook.h"

const int QD = 1;
const int FD_SIZE = 8192000;

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

#define ____cacheline_aligned __attribute__((__aligned__(64)))

struct Statistic {
    uint64_t sum = 0;
    uint64_t sum2 = 0;
    uint64_t cnt = 0;
    inline void add(int x) {
        sum += x;
        sum2 += 1LL * x * x;
        ++cnt;
    }

    inline double mean() const {
        return 1.0 * sum / cnt;
    }

    inline double std() const {
        return sqrt(1.0 * sum2 / cnt - mean() * mean());
    }
};
static Statistic statistic;

struct ____cacheline_aligned IOTask {
    IOType type;  // 0: read, 1: write
    int fd;
    void* buf;
    ssize_t nbytes;
    ssize_t return_value;
    butil::atomic<int>* butex;
    IOTask* next;

    void init() {
        butex = bthread::butex_create_checked<butil::atomic<int>>();
        next = nullptr;
    }

    void process() {
        const int expected_val = butex->load();
        add_to_mpsc(this);
        int rc = bthread::butex_wait(butex, expected_val, nullptr);
        if (rc < 0 && errno != 11) LOG(FATAL) << "butex error: " << strerror(errno);
    }

    void release() {
        bthread::butex_destroy(butex);
    }
};

inline void submit() {
    int ret = io_uring_submit(&ring);
    if (ret < 0) LOG(FATAL) << "io_uring_submit: " << strerror(-ret);
    statistic.add(ret);
}

inline IOTask* is_submit_complete(IOTask* old_head) {
    IOTask *desired = nullptr, *new_head = old_head;
    // attempt to do CAS
    if (mpsc_head.compare_exchange_strong(new_head, desired, butil::memory_order_acquire)) {
        return nullptr;
    }
    IOTask* at = new_head;
    IOTask* at_next = at->next;
    while (at_next == nullptr) sched_yield(), at_next = at->next;
    // reverse the list
    do {
        IOTask* temp_store = at_next->next;
        int hh = 0;
        while (at_next != old_head && temp_store == nullptr) sched_yield(), temp_store = at_next->next;
        at_next->next = at;
        at = at_next;
        at_next = temp_store;
    } while (at != old_head);
    new_head->next = nullptr;
    return new_head;
}

inline bool add_to_sq_no_wait(IOTask* task) {
    struct io_uring_sqe* sqe;
    sqe = io_uring_get_sqe(&ring);
    if (!sqe) return false;                              // there is no empty sqe
    sqe->user_data = reinterpret_cast<long long>(task);  // record task
    if (task->type == IOType::READ) {                    // read
        io_uring_prep_read(sqe, task->fd, task->buf, task->nbytes, fd_offset[task->fd]);
    } else if (task->type == IOType::WRITE) {  // write
        io_uring_prep_write(sqe, task->fd, task->buf, task->nbytes, fd_offset[task->fd]);
    }
    return true;
}

inline void waitfor_epoll(IOTask* task) {
    const int expected_val = epoll_butex->load();
    if (add_to_sq_no_wait(task)) return;
    submit();
    int rc = bthread::butex_wait(epoll_butex, expected_val, nullptr);
    if (rc < 0 && errno != 11) LOG(FATAL) << "butex error: " << strerror(errno);
    assert(add_to_sq_no_wait(task));
}

inline void loop_add_to_sq(IOTask* task, bool enable_wait) {
    if (!add_to_sq_no_wait(task)) {
        if (enable_wait) {
            waitfor_epoll(task);
        } else {
            start_keep_submit(task);
            return;
        }
    }
    do {
        IOTask* new_head;
        if (mpsc_head.load() == task) {
            submit();
            new_head = is_submit_complete(task);
            if (new_head == nullptr) return;
        } else {
            new_head = is_submit_complete(task);
        }
        IOTask* at = task->next;  // skip the first one
        while (at != nullptr) {   // try to submit all of these requests
            if (add_to_sq_no_wait(at)) {
                at = at->next;
            } else {
                if (enable_wait) {
                    IOTask* cache_next = at->next;
                    waitfor_epoll(at);
                    at = cache_next;
                } else {
                    submit();
                    start_keep_submit(at);  // start another bthread to submit them
                    return;                 // prevent blocking
                }
            }
        }
        task = new_head;
    } while (1);
}

// return new head
// nullptr: there is no new head
// others: the pointer to new head

static void* keep_submit(void* arg) {
    IOTask* tail = static_cast<IOTask*>(arg);
    // in bthread, keep submitting requests
    while (tail->next != nullptr) {
        if (!add_to_sq_no_wait(tail)) waitfor_epoll(tail);
        tail = tail->next;
    }
    loop_add_to_sq(tail, true);
    return nullptr;
}

inline void start_keep_submit(IOTask* tail) {
    bthread_start_background(&keep_submit_bthread, nullptr, keep_submit, tail);
}

inline void add_to_mpsc(IOTask* task) {
    IOTask* prev_head = mpsc_head.exchange(task, butil::memory_order_release);
    if (prev_head != nullptr) {
        task->next = prev_head;
        return;
    }
    // We've got the right to submit.
    task->next = nullptr;
    loop_add_to_sq(task);
}

inline bool is_brpc_co_environment() {
    return bthread_self() != 0;
}

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

static void __on_io_uring_epoll_handler(int events) {
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
                task->butex->fetch_add(1);
                bthread::butex_wake(task->butex);
            }
            io_uring_cq_advance(&ring, nr);
        } while (io_uring_peek_cqe(&ring, &cqe) == 0);
    }
}

__attribute__((constructor)) void library_init() {
    io_uring_queue_init(QD, &ring, 0);
    epoll_butex = bthread::butex_create_checked<butil::atomic<int>>();
    brpc::EventDispatcher& dispathcer = brpc::GetGlobalEventDispatcher(ring.ring_fd);
    dispathcer.RegisterIOuringHandler(__on_io_uring_epoll_handler);
    if (dispathcer.AddIOuringEpoll(ring.ring_fd)) {
        LOG(FATAL) << "Cannot add io_uring epoll instance.";
    }
    if (!dispathcer.Running()) brpc::GetGlobalEventDispatcher(ring.ring_fd).Start(nullptr);
    LOG(INFO) << "IO_Uring Hook successfully.";
}

__attribute__((destructor)) void library_cleanup() {
    io_uring_queue_exit(&ring);
    bthread::butex_destroy(epoll_butex);
    LOG(INFO) << "Submit Info: Mean=" << statistic.mean() << ", Std=" << statistic.std();
}

extern "C" int open(const char* __path, int __oflag, ...) {
    if (fn_open == nullptr) lock_fn_init();
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
    if (S_ISREG(statbuf.st_mode)) {
        is_file_fd[fd] = true;
        fd_offset[fd] = 0;
    }
    return fd;
}

extern "C" int close(int __fd) {
    if (fn_close == nullptr) lock_fn_init();
    is_file_fd[__fd] = false;
    return fn_close(__fd);
}

extern "C" ssize_t read(int __fd, void* __buf, size_t __nbytes) {
    if (fn_read == nullptr) lock_fn_init();
    if (is_file_fd[__fd] && is_brpc_co_environment()) {
        IOTask* task = new IOTask();
        task->init();
        task->type = IOType::READ, task->fd = __fd;
        task->buf = __buf, task->nbytes = __nbytes;
        task->process();
        fd_offset[__fd] += task->return_value;
        task->release();
        ssize_t ret = task->return_value;
        // butil::return_object(task);
        delete task;
        return ret;
    } else {
        ssize_t __bytes = fn_read(__fd, __buf, __nbytes);
        fd_offset[__fd] += __bytes;
        return __bytes;
    }
}

extern "C" ssize_t write(int __fd, const void* __buf, size_t __n) {
    if (fn_write == nullptr) lock_fn_init();
    if (is_file_fd[__fd] && is_brpc_co_environment()) {
        IOTask* task = butil::get_object<IOTask>();
        task->init();
        task->type = IOType::WRITE, task->fd = __fd;
        task->buf = const_cast<void*>(__buf), task->nbytes = __n;
        task->process();
        fd_offset[__fd] += task->return_value;
        task->release();
        ssize_t ret = task->return_value;
        butil::return_object(task);
        return ret;
    } else {
        ssize_t __bytes = fn_write(__fd, __buf, __n);
        fd_offset[__fd] += __bytes;
        return __bytes;
    }
}