#pragma once

#include <assert.h>
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <butil/string_printf.h>
#include <butil/strings/string_util.h>
#include <fcntl.h>

#include <condition_variable>
// #include <glog/logging.h>
#include <zlib.h>

#include <mutex>
#include <thread>

inline int64_t getCurrentTimestampNanos() {
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        now.time_since_epoch());
    return duration.count();
}

struct Counter {
    int64_t sum = 0;
    int cnt = 0;
    int64_t begin_time, end_time;
    char buf[100];
    const char* label;
    Counter(const char* msg) : label(msg) {}
    void start() { begin_time = getCurrentTimestampNanos(); }
    void end() {
        end_time = getCurrentTimestampNanos();
        sum += end_time - begin_time;
        ++cnt;
    }
    char* get() {
        sprintf(buf, "[%s: avg: %.2f ms, cnt: %d]", label, 1.0 * sum / cnt,
                cnt);
        return buf;
    }
};

namespace Util {
namespace IO {

namespace Exception {

struct Base : std::exception {
    virtual void FormatErrorMessage() const = 0;
    const char* what() const noexcept override {
        FormatErrorMessage();
        return exception_message.c_str();
    }

    mutable std::string exception_message;
};

struct WithFileName {
    WithFileName() {}
    void SetFileName(const std::string& arg_file_name) {
        file_name = arg_file_name;
    }
    std::string file_name;
};

struct WithFileLine {
    WithFileLine() { file_line = -1; }
    void SetFileLine(size_t arg_file_line) { file_line = arg_file_line; }
    size_t file_line;
};

struct WithErrno {
    WithErrno() { errno_value = 0; }
    void SetErrno(int arg_errno_value) { errno_value = arg_errno_value; }
    int errno_value;
};

struct CanNotOpenFile : Base, WithFileName, WithErrno {
    void FormatErrorMessage() const override {
        if (errno_value != 0) {
            butil::string_printf(&exception_message,
                                 "Can not open file %s because %s",
                                 file_name.c_str(), std::strerror(errno_value));
        } else {
            butil::string_printf(&exception_message, "Can not open file %s",
                                 file_name.c_str());
        }
    }
};

struct LineLengthLimitExceeded : Base, WithFileName, WithFileLine {
    void FormatErrorMessage() const override {
        butil::string_printf(
            &exception_message,
            "Line %ld in file %s exceeds the maximum length of buffer_size",
            file_line, file_name.c_str());
    }
};

}  // namespace Exception

class ByteSourceBase {
   public:
    virtual int Read(char* buffer, size_t size) = 0;
    virtual ~ByteSourceBase() {}
};

class GZipFileByteSource : public ByteSourceBase {
   public:
    Counter gzread_cnt;
    explicit GZipFileByteSource(gzFile fgz) : fgz_(fgz), gzread_cnt("gzread") {}
    ~GZipFileByteSource() { gzclose(fgz_); }
    int Read(char* buffer, size_t size) {
        if (gzeof(fgz_)) {
            return 0;
        }
        // gzread_cnt.start();
        int read_size = gzread(fgz_, buffer, (unsigned)size);
        // gzread_cnt.end();
        int gz_errno = 0;
        const char* gz_strerror = gzerror(fgz_, &gz_errno);

        if (gz_errno < 0) {
            std::string error_msg;
            butil::string_printf(&error_msg, "Read GZipFile error %d %s %s",
                                 gz_errno, gz_strerror, std::strerror(errno));
            throw std::runtime_error(error_msg);
        }
        return read_size;
    }

   private:
    gzFile fgz_{Z_NULL};
};

class UnixFileByteSource : public ByteSourceBase {
   public:
    explicit UnixFileByteSource(int file) : fd(file) {}
    ~UnixFileByteSource() { close(fd); }
    int Read(char* buffer, size_t size) {
        size_t read_size = read(fd, buffer, size);
        return static_cast<int>(read_size);
    }

   private:
    int fd;
};

class AsynchronousReader {
   public:
    Counter guard_cnt, wait_cnt;
    AsynchronousReader() : guard_cnt("guard"), wait_cnt("wait") {}
    static void* AsyncTask(void* arg) {
        AsynchronousReader* reader = static_cast<AsynchronousReader*>(arg);
        std::unique_lock<bthread::Mutex> guard_wlk(reader->lock_);
        try {
            for (;;) {
                while (!reader->termination_requested_ &&
                       reader->desired_byte_count_ == 0) {
                    reader->read_requested_condition_.wait(guard_wlk);
                }
                if (reader->termination_requested_) return nullptr;
                reader->read_byte_count_ = reader->byte_source_->Read(
                    reader->buffer_, reader->desired_byte_count_);
                reader->desired_byte_count_ = 0;
                reader->read_finished_condition_.notify_one();
            }
        } catch (...) {
            reader->read_error_ = std::current_exception();
        }
        reader->read_finished_condition_.notify_one();
        return nullptr;
    }
    void Init(std::unique_ptr<ByteSourceBase> arg_byte_source) {
        std::unique_lock<bthread::Mutex> guard(lock_);
        byte_source_ = std::move(arg_byte_source);
        CHECK_EQ(0, bthread_start_background(
                        &worker_, NULL, AsynchronousReader::AsyncTask, this));
    }

    bool Valid() const { return byte_source_ != nullptr; }

    void StartRead(char* arg_buffer, size_t arg_desired_byte_count) {
        CHECK_GT(arg_desired_byte_count, 0);
        std::unique_lock<bthread::Mutex> guard(lock_);
        buffer_ = arg_buffer;
        desired_byte_count_ = arg_desired_byte_count;
        read_byte_count_ = -1;
        read_requested_condition_.notify_one();
    }

    int FinishRead() {
        guard_cnt.start();
        std::unique_lock<bthread::Mutex> guard(lock_);
        guard_cnt.end();
        wait_cnt.start();
        while (!read_error_ && read_byte_count_ == -1) {
            read_finished_condition_.wait(guard);
        }
        wait_cnt.end();
        if (read_error_)
            std::rethrow_exception(read_error_);
        else
            return read_byte_count_;
    }

    ~AsynchronousReader() {
        if (byte_source_ != nullptr) {
            {
                std::unique_lock<bthread::Mutex> guard(lock_);
                termination_requested_ = true;
            }
            read_requested_condition_.notify_one();
            bthread_join(worker_, NULL);
        }
    }
    std::unique_ptr<ByteSourceBase> byte_source_;

   private:
    bool termination_requested_{false};
    std::exception_ptr read_error_;
    char* buffer_{nullptr};
    size_t desired_byte_count_{0};
    int read_byte_count_{-1};
    bthread_t worker_;
    bthread::Mutex lock_;
    bthread::ConditionVariable read_finished_condition_;
    bthread::ConditionVariable read_requested_condition_;
};

class SynchronousReader {
   public:
    void Init(std::unique_ptr<ByteSourceBase> arg_byte_source) {
        byte_source_ = std::move(arg_byte_source);
    }

    bool Valid() const { return byte_source_ != nullptr; }

    void StartRead(char* arg_buffer, size_t arg_desired_byte_count) {
        buffer_ = arg_buffer;
        desired_byte_count_ = arg_desired_byte_count;
    }

    int FinishRead() {
        // int64_t t1 = getCurrentTimestampNanos();
        int ret = byte_source_->Read(buffer_, desired_byte_count_);
        // int64_t t2 = getCurrentTimestampNanos();
        // sum += t2 - t1;
        // ++cnt;
        // if (cnt == 10000) {
        //     printf("%lld\n", sum / cnt);
        //     cnt = sum = 0;
        // }
        return ret;
    }

   private:
    std::unique_ptr<ByteSourceBase> byte_source_;
    char* buffer_{nullptr};
    size_t desired_byte_count_{0};
    int64_t sum = 0;
    int cnt = 0;
};

template <typename ParserType, typename ReaderType = SynchronousReader>
class LineReader {
   public:
    LineReader() = delete;
    LineReader(const LineReader&) = delete;
    LineReader& operator=(const LineReader&) = delete;

    explicit LineReader(const std::string& file_name, ParserType* parser,
                        size_t block_size = (1 << 13))
        : file_name_(file_name), parser_(parser), block_size_(block_size) {
        Init(OpenFile());
    }

    int ReadLine() {
        if (data_begin_ == data_end_) return 1;

        assert(data_begin_ < data_end_);
        assert(data_end_ <= block_size_ * 2);

        if (data_begin_ >= block_size_) {
            std::memcpy(buffer_.get(), buffer_.get() + block_size_,
                        block_size_);
            data_begin_ -= block_size_;
            data_end_ -= block_size_;
            if constexpr (std::is_same<ReaderType, SynchronousReader>::value) {
                if (reader_.Valid()) {
                    data_end_ += reader_.FinishRead();
                    reader_.StartRead(buffer_.get() + block_size_, block_size_);
                }
            } else if constexpr (std::is_same<ReaderType,
                                              AsynchronousReader>::value) {
                if (reader_.Valid()) {
                    data_end_ += reader_.FinishRead();
                    std::memcpy(buffer_.get() + block_size_,
                                buffer_.get() + 2 * block_size_, block_size_);
                    reader_.StartRead(buffer_.get() + 2 * block_size_,
                                      block_size_);
                }
            } else {
                CHECK(false) << "Unsupported ReaderType";
            }
        }

        int ret = parser_->BeginOfRow(file_line_);
        if (ret < 0) {
            return ret;
        }

        size_t field_begin = data_begin_;
        size_t field_num = 0, line_end = data_begin_;

        while (line_end != data_end_) {
            char& ch = buffer_[line_end];
            if (ch > ',') {
                // do nothing
            } else if (ch == ',') {
                ch = '\0';
                ret = parser_->LoadValue(file_line_, field_num++,
                                         buffer_.get() + field_begin,
                                         line_end - field_begin);
                if (ret < 0) {
                    return ret;
                }
                field_begin = line_end + 1;
            } else if (ch == '\n') {
                break;
            } else {
                // do nothing
            }
            ++line_end;
        }

        if (line_end - data_begin_ + 1 > block_size_) {
            Exception::LineLengthLimitExceeded exception;
            exception.SetFileName(file_name_);
            exception.SetFileLine(file_line_);
            throw exception;
        }

        if (line_end != data_end_ && buffer_[line_end] == '\n') {
            buffer_[line_end] = '\0';
        } else {
            // some files are missing the newline at the end of the last line
            ++data_end_;
            buffer_[line_end] = '\0';
        }
        size_t field_end = line_end;
        if (line_end != data_begin_ && buffer_[line_end - 1] == '\r') {
            buffer_[line_end - 1] = '\0';
            field_end = line_end - 1;
        }

        ret = parser_->LoadValue(file_line_, field_num++,
                                 buffer_.get() + field_begin,
                                 field_end - field_begin);
        if (ret < 0) {
            return ret;
        }

        if (parser_->EndOfRow(file_line_) < 0) {
            return -1;
        }

        data_begin_ = line_end + 1;
        ++file_line_;
        return 0;
    }
    ReaderType reader_;

   private:
    std::unique_ptr<ByteSourceBase> OpenFile() {
        if (!file_name_.empty()) {
            if (EndsWith(file_name_, ".gz", true)) {
                gzFile fgz = gzopen(file_name_.c_str(), "rb");
                if (fgz == Z_NULL) {
                    int tmp = errno;
                    Exception::CanNotOpenFile exception;
                    exception.SetErrno(tmp);
                    exception.SetFileName(file_name_);
                    throw exception;
                }
                return std::unique_ptr<ByteSourceBase>(
                    new GZipFileByteSource(fgz));
            } else {
                int fd = open(file_name_.c_str(), O_RDONLY | O_DIRECT);
                if (fd < 0) {
                    int tmp = errno;
                    Exception::CanNotOpenFile exception;
                    exception.SetErrno(tmp);
                    exception.SetFileName(file_name_);
                    throw exception;
                }
                return std::unique_ptr<ByteSourceBase>(
                    new UnixFileByteSource(fd));
            }
        } else {
            file_name_ = "stdin";
            return std::unique_ptr<ByteSourceBase>(new UnixFileByteSource(0));
        }
    }

    void Init(std::unique_ptr<ByteSourceBase> byte_source) {
        void* buf;
        posix_memalign(&buf, 4096, 3 * block_size_);
        buffer_ = std::unique_ptr<char[]>((char*)buf);
        data_end_ = byte_source->Read(buffer_.get(), 2 * block_size_);
        if (data_end_ == 2 * block_size_) {
            reader_.Init(std::move(byte_source));
            if constexpr (std::is_same<ReaderType, SynchronousReader>::value) {
                reader_.StartRead(buffer_.get() + block_size_, block_size_);
            } else if constexpr (std::is_same<ReaderType,
                                              AsynchronousReader>::value) {
                reader_.StartRead(buffer_.get() + 2 * block_size_, block_size_);
            } else {
                CHECK(false) << "Unsupported ReaderType";
            }
        }
    }

   private:
    std::unique_ptr<char[]> buffer_;
    size_t data_begin_{0};
    size_t data_end_{0};
    std::string file_name_;
    size_t file_line_{0};
    ParserType* parser_{nullptr};
    size_t block_size_{0};
};

}  // namespace IO
}  // namespace Util
