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
    cnt.fetch_add(1);
}

void IOTask::unref() {
    int res = cnt.fetch_sub(1);
    if (res == 1) release();
}

void IOTask::release() {
    if (cnt.load() != 0) {
        printf("%d\n", cnt.load());
    }
    next = nullptr;
    bthread::butex_destroy(butex);
    // printf("return back %p\n", this);
    butil::return_object(this);
}

// IOTask::__IOTaskPtr::__IOTaskPtr(IOTask* p) {
//     if (p) p->enter();
//     ptr = p;
// }
// IOTask::__IOTaskPtr::__IOTaskPtr(const __IOTaskPtr& p) {
//     if (p.ptr) p.ptr->enter();
//     ptr = p.ptr;
// }
// const IOTask::__IOTaskPtr& IOTask::__IOTaskPtr::operator=(const __IOTaskPtr& p) {
//     if (p.ptr) p.ptr->enter();
//     if (ptr) ptr->leave();
//     ptr = p.ptr;
//     return (*this);
// }
// IOTask* IOTask::__IOTaskPtr::get() const {
//     return ptr;
// }
// IOTask::__IOTaskPtr::~__IOTaskPtr() {
//     if (ptr) ptr->leave();
// }
// IOTask* IOTask::__IOTaskPtr::operator->() {
//     return ptr;
// }

void IOTask::wait_process() {
    const int expected_val = butex->load();
    add_to_mpsc(this);
    // assert(this->flag.load());
    // printf("wait %d %p\n", bthread_self(), this);
    int rc = bthread::butex_wait(butex, expected_val, nullptr);
    if (rc < 0 && errno != 11) LOG(FATAL) << "butex error: " << strerror(errno);
}

IOTask* IOTask::init() {
    butex = bthread::butex_create_checked<butil::atomic<int>>();
    next = nullptr;
    cnt.store(0);
    return this;
}

void IOTask::notify() {
    butex->fetch_add(1);
    // printf("notify %p\n", this);
    bthread::butex_wake(butex);
}

inline void submit() {
    int ret = io_uring_submit(&ring);
    if (ret < 0) LOG(FATAL) << "io_uring_submit: " << strerror(-ret);
    statistic.add(ret);
}

inline IOTask* is_submit_complete(IOTask* old_head, IOTask*& new_tail) {
    IOTask *desired = nullptr, *__new_head = old_head;
    // printf("is there newer head than %p?\n", __new_head);
    //     attempt to do CAS
    if (mpsc_head.compare_exchange_strong(__new_head, desired, butil::memory_order_acquire)) {
        // printf("set head to null cause head = %p\n", old_head.get());
        return nullptr;
    }
    // printf("find newer head: %p\n", __new_head);
    IOTask* new_head = __new_head;
    IOTask* at = new_head;
    IOTask* at_next = at->next;
    while (at_next == nullptr) sched_yield(), at_next = at->next;
    // reverse the list
    while (at_next != old_head) {
        IOTask* temp_store = at_next->next;
        while (temp_store == nullptr) sched_yield(), temp_store = at_next->next;
        // printf("relink %p->%p\n", at_next.get(), at.get());
        at_next->next = at;
        at = at_next;
        at_next = temp_store;
    };
    new_tail = at;
    new_head->next = nullptr;
    return new_head;
}

inline void loop_add_to_sq(IOTask* task, bool enable_wait) {
    // printf("submit from %p\n", task.get());
    if (!add_to_sq_no_wait(task)) {
        if (enable_wait) {
            waitfor_epoll(task);
        } else {
            start_keep_submit(task);
            return;
        }
    }
    do {
        IOTask *new_head, *cache_next;
        if (mpsc_head.load() == task) {
            submit();
            new_head = is_submit_complete(task, cache_next);
            if (new_head == nullptr) {
                task->unref();
                return;
            }
        } else {
            new_head = is_submit_complete(task, cache_next);
        }
        task->unref();
        IOTask* at = cache_next;
        // printf("again submit from %p.next = %p\n", task.get(), at.get());
        while (at != nullptr) {  // try to submit all of these requests
            IOTask* pre_at = at;
            if (add_to_sq_no_wait(at)) {
                at = at->next;
            } else {
                if (enable_wait) {
                    waitfor_epoll(at);
                    at = at->next;
                } else {
                    start_keep_submit(at);  // start another bthread to submit them
                    return;                 // prevent blocking
                }
            }
            if (pre_at != new_head) pre_at->unref();
        }
        task = new_head;
    } while (1);
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
    loop_add_to_sq(task);
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
    task->ref();
    // printf("sqe %p\n", task.get());
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

// return new head
// nullptr: there is no new head
// others: the pointer to new head

static void* keep_submit(void* arg) {
    IOTask* tail = static_cast<IOTask*>(arg);
    // printf("keep submit from %p\n", tail.get());
    //     in bthread, keep submitting requests
    while (tail->next != nullptr) {
        if (!add_to_sq_no_wait(tail)) waitfor_epoll(tail);
        IOTask* pre_tail = tail;
        tail = tail->next;
        pre_tail->unref();
    }
    loop_add_to_sq(tail, true);
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
        // printf("read %d %p %d\n", bthread_self(), task.get(), task->cnt.load());
        task->type = IOType::READ, task->fd = __fd;
        task->buf = __buf, task->nbytes = __nbytes;
        task->wait_process();
        fd_offset[__fd] += task->return_value;
        ssize_t ret = task->return_value;
        // printf("finish %d %p\n", bthread_self(), task.get());
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