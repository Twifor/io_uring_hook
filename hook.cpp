#include "hook.h"

void Statistic::add(int x) {
    sum += x;
    sum2 += 1LL * x * x;
    ++cnt;
}
double Statistic::mean() const {
    return 1.0 * sum / cnt;
}

double Statistic::std() const {
    return sqrt(1.0 * sum2 / cnt - mean() * mean());
}

void IOTask::ref() {
    cnt.fetch_add(1, butil::memory_order_relaxed);
}

void IOTask::unref() {
    int res = cnt.fetch_sub(1, butil::memory_order_relaxed);
    if (res == 1) release();
}

void IOTask::release() {
    assert(cnt.load() == 0);
    next = nullptr;
    bthread::butex_destroy(butex);
    // printf("return back %p\n", this);
    butil::return_object(this);
}

void IOTask::wait_process() {
    const int expected_val = butex->load(butil::memory_order_acquire);
    add_to_mpsc(this);
    // assert(this->flag.load());
    // printf("wait %d %p\n", bthread_self(), this);
    int rc = bthread::butex_wait(butex, expected_val, nullptr);
    if (rc < 0 && errno != 11) LOG(FATAL) << "butex error: " << strerror(errno);
}

IOTask* IOTask::init() {
    butex = bthread::butex_create_checked<butil::atomic<int>>();
    next = nullptr;
    cnt.store(0, butil::memory_order_release);
    return this;
}

void IOTask::notify() {
    butex->fetch_add(1, butil::memory_order_release);
    // printf("notify %p\n", this);
    bthread::butex_wake(butex);
}

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

inline bool is_submit_complete(IOTask* old_head, bool is_cur_task_resolved) {
    IOTask *desired = nullptr, *new_head = old_head;
    if (!is_cur_task_resolved) desired = old_head;
    // attempt to do CAS
    if (mpsc_head.compare_exchange_strong(new_head, desired, butil::memory_order_acquire)) {
        return true;
    }
    IOTask* at = new_head;
    IOTask* at_next = at->next;
    while (at_next == nullptr) sched_yield(), at_next = at->next;
    do {
        IOTask* cached_next = at_next->next;
        while (at_next != old_head && cached_next == nullptr) sched_yield(), cached_next = at_next->next;
        at_next->next = at;
        at = at_next;
        at_next = cached_next;
    } while (at != old_head);
    new_head->next = nullptr;
    return false;
}

inline void loop_add_to_sq(IOTask* old_head, bool enable_wait) {
resubmit:
    IOTask *at = old_head, *pre_head = nullptr;
    do {
        while (at) {
            if (!add_to_sq(at, enable_wait)) return;
            if (pre_head) pre_head->unref();
            pre_head = at;
            at = at->next;
        }
        if (is_submit_complete(pre_head, false)) {
            submit();
            break;
        }
        at = pre_head->next;
        pre_head->unref();
        pre_head = nullptr;
    } while (1);
    if (!is_submit_complete(pre_head, true)) {
        IOTask* pre_head_next = pre_head->next;
        pre_head->unref();
        if (enable_wait) {
            old_head = pre_head_next;
            goto resubmit;
        } else {
            start_keep_submit(pre_head_next);
        }
    } else {
        pre_head->unref();
    }
}

inline void add_to_mpsc(IOTask* task) {
    IOTask* prev_head = mpsc_head.exchange(task, butil::memory_order_release);
    // printf("link %p->%p\n", task.get(), prev_head.get());
    task->ref();
    if (prev_head != nullptr) {
        task->next = prev_head;
        return;
    }
    // We've got the right to submit.
    task->next = nullptr;
    loop_add_to_sq(task, false);
}

inline bool add_to_sq(IOTask* task, bool enable_wait) {
    struct io_uring_sqe* sqe;
    const int expected_val = epoll_butex->load(butil::memory_order_acquire);
    sqe = io_uring_get_sqe(&ring);
    if (!sqe) {  // there is no empty sqe
        if (enable_wait) {
            submit();
            int rc = bthread::butex_wait(epoll_butex, expected_val, nullptr);
            if (rc < 0 && errno != 11) LOG(FATAL) << "butex error: " << strerror(errno);
            assert(sqe = io_uring_get_sqe(&ring));
        } else {
            start_keep_submit(task);
            return false;
        }
    }
    sqe->user_data = reinterpret_cast<long long>(task);  // record task
    if (task->type == IOType::READ) {                    // read
        io_uring_prep_read(sqe, task->fd, task->buf, task->nbytes, fd_offset[task->fd]);
    } else if (task->type == IOType::WRITE) {  // write
        io_uring_prep_write(sqe, task->fd, task->buf, task->nbytes, fd_offset[task->fd]);
    }
    task->ref();
    // printf("sqe %p\n", task.get());
    return true;
}

// return new head
// nullptr: there is no new head
// others: the pointer to new head

static void* keep_submit(void* arg) {
    // printf("keep submit from %p\n", tail.get());
    // in bthread, keep submitting requests
    loop_add_to_sq(static_cast<IOTask*>(arg), true);
    return nullptr;
}

inline void start_keep_submit(IOTask* tail) {
    bthread_start_background(&keep_submit_bthread, nullptr, keep_submit, tail);
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
    io_uring_queue_init(QD, &ring, 0);
    epoll_butex = bthread::butex_create_checked<butil::atomic<int>>();
    brpc::EventDispatcher& dispathcer = brpc::GetGlobalEventDispatcher(ring.ring_fd);
    dispathcer.RegisterIOuringHandler(on_io_uring_epoll_handler);
    if (dispathcer.AddIOuringEpoll(ring.ring_fd)) {
        LOG(FATAL) << "Cannot add io_uring epoll instance.";
    }
    if (!dispathcer.Running()) brpc::GetGlobalEventDispatcher(ring.ring_fd).Start(nullptr);
    LOG(DEBUG) << "IO_Uring Hook successfully.";
}

__attribute__((destructor)) void library_cleanup() {
    io_uring_queue_exit(&ring);
    bthread::butex_destroy(epoll_butex);
    LOG(DEBUG) << "Submit Info: Mean=" << statistic.mean() << ", Std=" << statistic.std();
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
    if (fn_write == nullptr) lock_fn_init();
    if (is_file_fd[__fd] && is_brpc_co_environment()) {
        IOTask* task = butil::get_object<IOTask>()->init();
        task->type = IOType::WRITE, task->fd = __fd;
        task->buf = const_cast<void*>(__buf), task->nbytes = __nbytes;
        task->wait_process();
        fd_offset[__fd] += task->return_value;
        ssize_t ret = task->return_value;
        return ret;
    } else {
        ssize_t __bytes = fn_write(__fd, __buf, __nbytes);
        fd_offset[__fd] += __bytes;
        return __bytes;
    }
}