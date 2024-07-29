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

const int QD = 256;
const int FD_SIZE = 4096;
const int EPOLL_OUT_FD = FD_SIZE - 1;  // reserved position

#define IOURING_SUBMIT                                                    \
    {                                                                     \
        int ret = io_uring_submit(&ring);                                 \
        if (ret < 0) LOG(FATAL) << "io_uring_submit: " << strerror(-ret); \
        ;                                                                 \
    }

static bool is_file_fd[FD_SIZE];                     // is a file? (not socket)
static bthread::ConditionVariable* b_cond[FD_SIZE];  // condition variables
static bthread::Mutex* b_mutex[FD_SIZE];             // mutex
static uint64_t fd_offset[FD_SIZE];                  // offset of files
static ssize_t fd_ret[FD_SIZE];                      // return value

// original functions:
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

struct IO_task {
    int type;  // 0: read, 1: write
    int fd;
    void* buf;
    ssize_t nbytes;
    IO_task* next;
    IO_task() : next(nullptr) {}
};
static struct io_uring ring;
static butil::atomic<IO_task*> mpsc_head;
static IO_task *mpsc_submit_tail, *mpsc_submit_head;
static bthread_t keep_submit_bthread;

inline bool add_to_sq_no_wait(IO_task* task) {
    struct io_uring_sqe* sqe;
    sqe = io_uring_get_sqe(&ring);
    if (!sqe) return false;     // there is no empty sqe
    sqe->user_data = task->fd;  // record fd
    if (task->type == 0) {      // read
        io_uring_prep_read(sqe, task->fd, task->buf, task->nbytes, fd_offset[task->fd]);
    } else if (task->type == 1) {  // write
        io_uring_prep_write(sqe, task->fd, task->buf, task->nbytes, fd_offset[task->fd]);
    }
    // printf("add to sq %d\n", task->fd);
    butil::return_object<IO_task>(task);  // delete request object
    return true;
}

// return new head
inline IO_task* is_submit_complete(IO_task* old_head) {
    IO_task* desired = nullptr;
    // attempt to do CAS
    if (mpsc_head.compare_exchange_strong(old_head, desired, butil::memory_order_acquire)) {
        return nullptr;
    }
    return old_head;
}

static void* keep_submit(void* arg) {
    printf("do we really need keep_submit?\n");
    // in bthread, keep submitting requests
    // submit [tail, head)
    while (mpsc_submit_tail != nullptr && mpsc_submit_tail != mpsc_submit_head) {
        if (add_to_sq_no_wait(mpsc_submit_tail)) {
            mpsc_submit_tail = mpsc_submit_tail->next;
        } else {
            std::unique_lock<bthread::Mutex> lock(*b_mutex[EPOLL_OUT_FD]);
            if (add_to_sq_no_wait(mpsc_submit_tail)) {  // try again
                mpsc_submit_tail = mpsc_submit_tail->next;
            } else {
                IOURING_SUBMIT
                b_cond[EPOLL_OUT_FD]->wait(lock);  // waiting for epollout
            }
        }
    }
    // submit head
    while (1) {
        if (add_to_sq_no_wait(mpsc_submit_head)) {
            break;
        } else {
            std::unique_lock<bthread::Mutex> lock(*b_mutex[EPOLL_OUT_FD]);
            if (add_to_sq_no_wait(mpsc_submit_head)) {
                break;
            } else {
                IOURING_SUBMIT
                b_cond[EPOLL_OUT_FD]->wait(lock);
            }
        }
    }

    // check new head
    do {
        IO_task *at, *at_next;
        if (mpsc_head.load() == mpsc_submit_head) {
            IOURING_SUBMIT
            mpsc_submit_head = is_submit_complete(mpsc_submit_head);
            if (mpsc_submit_head == nullptr) {
                mpsc_submit_tail = nullptr;
                return nullptr;
            }
        } else {
            mpsc_submit_head = is_submit_complete(mpsc_submit_head);
        }
        at = mpsc_submit_head;
        at_next = at->next;
        while (at_next != mpsc_submit_head) {
            IO_task* temp_store = at_next->next;
            at_next->next = at;
            at = at_next;
            at_next = temp_store;
        }
        mpsc_submit_head->next = nullptr;
        while (at != nullptr) {
            mpsc_submit_tail = at;
            if (add_to_sq_no_wait(at)) {
                at = at->next;
            } else {
                std::unique_lock<bthread::Mutex> lock(*b_mutex[EPOLL_OUT_FD]);
                if (add_to_sq_no_wait(at)) {
                    at = at->next;
                } else {
                    IOURING_SUBMIT
                    b_cond[EPOLL_OUT_FD]->wait(lock);
                }
            }
        }
    } while (1);
}

inline void start_keep_submit() {
    bthread_start_background(&keep_submit_bthread, nullptr, keep_submit, nullptr);
}

inline void add_to_mpsc(IO_task* task) {
    IO_task* prev_head = mpsc_head.load();
    do {
        task->next = prev_head;
    } while (!mpsc_head.compare_exchange_strong(prev_head, task, butil::memory_order_acquire));
    if (prev_head != nullptr) return;
    // We've got the right to submit.
    task->next = nullptr;
    if (!add_to_sq_no_wait(task)) {
        // failed
        mpsc_submit_tail = nullptr;
        mpsc_submit_head = task;
        start_keep_submit();
        return;
    }
    // usleep(16);
    do {
        IO_task *at, *at_next;
        if (mpsc_head.load() == task) {
            IOURING_SUBMIT
            mpsc_submit_head = is_submit_complete(task);
            if (mpsc_submit_head == nullptr) {
                mpsc_submit_tail = nullptr;  // finish
                return;
            }
        } else {
            mpsc_submit_head = is_submit_complete(task);
        }
        at = mpsc_submit_head;
        at_next = at->next;
        // reverse the list
        while (at_next != task) {
            IO_task* temp_store = at_next->next;
            at_next->next = at;
            at = at_next;
            at_next = temp_store;
        }
        task = mpsc_submit_head;
        task->next = nullptr;
        // list: at -> task
        while (at != nullptr) {  // try to submit all of these requests
            mpsc_submit_tail = at;
            if (add_to_sq_no_wait(at)) {
                at = at->next;
            } else {
                IOURING_SUBMIT
                start_keep_submit();  // start another bthread to submit them
                return;               // prevent blocking
            }
        }
    } while (1);
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
        std::unique_lock<bthread::Mutex> lock(*b_mutex[EPOLL_OUT_FD]);
        b_cond[EPOLL_OUT_FD]->notify_one();
    }
    if (events & EPOLLIN) {
        static struct io_uring_cqe* cqes[QD];
        unsigned int ret = io_uring_peek_batch_cqe(&ring, cqes, QD);
        for (int i = 0; i < ret; i++) {
            int fd = cqes[i]->user_data;
            {
                std::unique_lock lock(*b_mutex[fd]);
                fd_offset[fd] += cqes[i]->res;
                fd_ret[fd] = cqes[i]->res;
                b_cond[fd]->notify_one();
                // printf("notify! %d\n", fd);
            }
            io_uring_cqe_seen(&ring, cqes[i]);
        }
    }
}

__attribute__((constructor)) void library_init() {
    io_uring_queue_init(QD, &ring, 0);
    b_cond[EPOLL_OUT_FD] = new bthread::ConditionVariable();
    b_mutex[EPOLL_OUT_FD] = new bthread::Mutex();
    brpc::EventDispatcher& dispathcer = brpc::GetGlobalEventDispatcher(ring.ring_fd);
    dispathcer.RegisterIOuringHandler(__on_io_uring_epoll_handler);
    if (dispathcer.AddIOuringEpoll(ring.ring_fd)) {
        LOG(FATAL) << "Cannot add io_uring epoll instance.";
    }
    if (!dispathcer.Running()) brpc::GetGlobalEventDispatcher(ring.ring_fd).Start(nullptr);
}

__attribute__((destructor)) void library_cleanup() {
    io_uring_queue_exit(&ring);
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
        b_cond[fd] = new bthread::ConditionVariable();
        b_mutex[fd] = new bthread::Mutex();
        fd_offset[fd] = 0;
    }
    return fd;
}

extern "C" int close(int __fd) {
    if (fn_close == nullptr) lock_fn_init();
    if (is_file_fd[__fd]) {
        delete b_cond[__fd];
        delete b_mutex[__fd];
    }
    is_file_fd[__fd] = false;
    return fn_close(__fd);
}

extern "C" ssize_t read(int __fd, void* __buf, size_t __nbytes) {
    if (fn_read == nullptr) lock_fn_init();
    if (is_file_fd[__fd] && is_brpc_co_environment()) {
        IO_task* task = butil::get_object<IO_task>();
        task->type = 0;
        task->fd = __fd;
        task->buf = __buf;
        task->nbytes = __nbytes;
        {
            std::unique_lock lock(*b_mutex[__fd]);
            add_to_mpsc(task);
            b_cond[__fd]->wait(lock);
        }
        return fd_ret[__fd];
    } else {
        ssize_t __bytes = fn_read(__fd, __buf, __nbytes);
        fd_offset[__fd] += __bytes;
        return __bytes;
    }
}

extern "C" ssize_t write(int __fd, const void* __buf, size_t __n) {
    if (fn_write == nullptr) lock_fn_init();
    if (is_file_fd[__fd] && is_brpc_co_environment()) {
        IO_task* task = butil::get_object<IO_task>();
        task->type = 0;
        task->fd = __fd;
        task->buf = const_cast<void*>(__buf);
        task->nbytes = __n;
        {
            std::unique_lock lock(*b_mutex[__fd]);
            add_to_mpsc(task);
            b_cond[__fd]->wait(lock);
        }
        return fd_ret[__fd];
    } else {
        ssize_t __bytes = fn_write(__fd, __buf, __n);
        fd_offset[__fd] += __bytes;
        return __bytes;
    }
}