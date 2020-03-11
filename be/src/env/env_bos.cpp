// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "env/env.h"

#include <string>

#include "bcesdk/bos/client.h"

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "util/doris_metrics.h"
#include "util/errno.h"
#include "util/runtime_profile.h"

namespace doris {

// See https://cloud.baidu.com/doc/BOS/s/wjwvys6xi
// Baidu BOS supports the following 3 methods to upload a file:
// 1. Simple Normal Upload
// 2. Appendable File
// 3. Multipart Upload
//
// For now, we only use "Simple Normal Upload".
//
// NOTE:
// When opening a file for reading, if the file does not exist locally, it will be downloaded
// from BOS first, and subsequent reading is performed on the local file.
// When open a file for writing data, the data is first written to a local file and
// uploaded to BOS when closed.

namespace bossdk = baidu::bos::cppsdk;

using std::string;
using strings::Substitute;

// TODO(lingbin): modify to BOS_RPC_ERROR?
#define ENV_RETRUN_NOT_OK(int_expr) do {                                                     \
    int _ret_ = int_expr;                                                                    \
    if (_ret_ != 0) {                                                                        \
        return Status::ThriftRpcError(strings::Substitute("bos op error. errno=$0", _ret_)); \
    }                                                                                        \
} while (0)

static const std::string AK = "069fc2786e464e63a5f1183824ddb522";
static const std::string SK = "700adb0c695f4639bde274d59eaa980a";
static const std::string BUCKET = "bj-for-lingbin";
static bossdk::ClientOptions bos_options;

static Status io_error(const std::string& context, int err_number) {
    switch (err_number) {
    case EACCES:
    case ELOOP:
    case ENAMETOOLONG:
    case ENOENT:
    case ENOTDIR:
        return Status::NotFound(context, err_number, errno_to_string(err_number));
    case EEXIST:
        return Status::AlreadyExist(context, err_number, errno_to_string(err_number));
    case EOPNOTSUPP:
    case EXDEV: // No cross FS links allowed
        return Status::NotSupported(context, err_number, errno_to_string(err_number));
    case EIO:
        LOG(ERROR) << "I/O error, context=" << context;
    }
    return Status::IOError(context, err_number, errno_to_string(err_number));
}

static Status do_sync(int fd, const string& filename) {
    if (fdatasync(fd) < 0) {
        return io_error(filename, errno);
    }
    return Status::OK();
}

static Status do_open(const string& filename, Env::OpenMode mode, int* fd) {
    int flags = O_RDWR;
    switch (mode) {
    case Env::CREATE_OR_OPEN_WITH_TRUNCATE:
        flags |= O_CREAT | O_TRUNC;
        break;
    case Env::CREATE_OR_OPEN:
        flags |= O_CREAT;
        break;
    case Env::MUST_CREATE:
        flags |= O_CREAT | O_EXCL;
        break;
    case Env::MUST_EXIST:
        break;
    default:
        return Status::NotSupported(Substitute("Unknown create mode $0", mode));
    }
    int f;
    RETRY_ON_EINTR(f, open(filename.c_str(), flags, 0666));
    if (f < 0) {
        return io_error(filename, errno);
    }
    *fd = f;
    return Status::OK();
}

static Status do_readv_at(int fd, const std::string& filename, uint64_t offset,
                          const Slice* res, size_t res_cnt) {
    // Convert the results into the iovec vector to request
    // and calculate the total bytes requested
    size_t bytes_req = 0;
    struct iovec iov[res_cnt];
    for (size_t i = 0; i < res_cnt; i++) {
        const Slice& result = res[i];
        bytes_req += result.size;
        iov[i] = { result.data, result.size };
    }

    uint64_t cur_offset = offset;
    size_t completed_iov = 0;
    size_t rem = bytes_req;
    while (rem > 0) {
        // Never request more than IOV_MAX in one request
        size_t iov_count = std::min(res_cnt - completed_iov, static_cast<size_t>(IOV_MAX));
        ssize_t r;
        RETRY_ON_EINTR(r, preadv(fd, iov + completed_iov, iov_count, cur_offset));
        if (PREDICT_FALSE(r < 0)) {
            // An error: return a non-ok status.
            return io_error(filename, errno);
        }

        if (PREDICT_FALSE(r == 0)) {
            return Status::EndOfFile(
                Substitute("EOF trying to read $0 bytes at offset $1", bytes_req, offset));
        }

        if (PREDICT_TRUE(r == rem)) {
            // All requested bytes were read. This is almost always the case.
            return Status::OK();
        }
        DCHECK_LE(r, rem);
        // Adjust iovec vector based on bytes read for the next request
        ssize_t bytes_rem = r;
        for (size_t i = completed_iov; i < res_cnt; i++) {
            if (bytes_rem >= iov[i].iov_len) {
                // The full length of this iovec was read
                completed_iov++;
                bytes_rem -= iov[i].iov_len;
            } else {
                // Partially read this result.
                // Adjust the iov_len and iov_base to request only the missing data.
                iov[i].iov_base = static_cast<uint8_t *>(iov[i].iov_base) + bytes_rem;
                iov[i].iov_len -= bytes_rem;
                break; // Don't need to adjust remaining iovec's
            }
        }
        cur_offset += r;
        rem -= r;
    }
    DCHECK_EQ(0, rem);
    return Status::OK();
}

static Status do_writev_at(int fd, const string& filename, uint64_t offset,
                           const Slice* data, size_t data_cnt, size_t* bytes_written) {
    // Convert the results into the iovec vector to request
    // and calculate the total bytes requested.
    size_t bytes_req = 0;
    struct iovec iov[data_cnt];
    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];
        bytes_req += result.size;
        iov[i] = { result.data, result.size };
    }

    uint64_t cur_offset = offset;
    size_t completed_iov = 0;
    size_t rem = bytes_req;
    while (rem > 0) {
        // Never request more than IOV_MAX in one request.
        size_t iov_count = std::min(data_cnt - completed_iov, static_cast<size_t>(IOV_MAX));
        ssize_t w;
        RETRY_ON_EINTR(w, pwritev(fd, iov + completed_iov, iov_count, cur_offset));
        if (PREDICT_FALSE(w < 0)) {
            // An error: return a non-ok status.
            return io_error(filename, errno);
        }

        if (PREDICT_TRUE(w == rem)) {
            // All requested bytes were read. This is almost always the case.
            rem = 0;
            break;
        }
        // Adjust iovec vector based on bytes read for the next request.
        ssize_t bytes_rem = w;
        for (size_t i = completed_iov; i < data_cnt; i++) {
            if (bytes_rem >= iov[i].iov_len) {
                // The full length of this iovec was written.
                completed_iov++;
                bytes_rem -= iov[i].iov_len;
            } else {
                // Partially wrote this result.
                // Adjust the iov_len and iov_base to write only the missing data.
                iov[i].iov_base = static_cast<uint8_t *>(iov[i].iov_base) + bytes_rem;
                iov[i].iov_len -= bytes_rem;
                break; // Don't need to adjust remaining iovec's.
            }
        }
        cur_offset += w;
        rem -= w;
    }
    DCHECK_EQ(0, rem);
    *bytes_written = bytes_req;
    return Status::OK();
}

class BosWritableFile : public WritableFile {
public:
    BosWritableFile(std::string filename, int fd, uint64_t filesize, bool sync_on_close) :
            _filename(std::move(filename)), _fd(fd),
            _sync_on_close(sync_on_close), _filesize(filesize) { }

    ~BosWritableFile() override {
        WARN_IF_ERROR(close(), "Failed to close file, file=" + _filename);
    }

    Status append(const Slice& data) override {
        return appendv(&data, 1);
    }

    Status appendv(const Slice* data, size_t cnt) override {
        size_t bytes_written = 0;
        RETURN_IF_ERROR(do_writev_at(_fd, _filename, _filesize, data, cnt, &bytes_written));
        _filesize += bytes_written;
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override {
        uint64_t offset = std::max(_filesize, _pre_allocated_size);
        int ret;
        RETRY_ON_EINTR(ret, fallocate(_fd, 0, offset, size));
        if (ret != 0) {
            if (errno == EOPNOTSUPP) {
                LOG(WARNING) << "The filesystem does not support fallocate().";
            } else if (errno == ENOSYS) {
                LOG(WARNING) << "The kernel does not implement fallocate().";
            } else {
                return io_error(_filename, errno);
            }
        }
        _pre_allocated_size = offset + size;
        return Status::OK();
    }

    Status close() override {
        if (_closed) {
            return Status::OK();
        }
        Status s;

        // If we've allocated more space than we used, truncate to the
        // actual size of the file and perform Sync().
        if (_filesize < _pre_allocated_size) {
            int ret;
            RETRY_ON_EINTR(ret, ftruncate(_fd, _filesize));
            if (ret != 0) {
                s = io_error(_filename, errno);
                _pending_sync = true;
            }
        }

        if (_sync_on_close) {
            Status sync_status = sync();
            if (!sync_status.ok()) {
                LOG(ERROR) << "Unable to Sync " << _filename << ": " << sync_status.to_string();
                if (s.ok()) {
                    s = sync_status;
                }
            }
        }

        int ret;
        RETRY_ON_EINTR(ret, ::close(_fd));
        if (ret < 0) {
            if (s.ok()) {
                s = io_error(_filename, errno);
            }
        }

        _closed = true;
        return s;
    }

    Status flush(FlushMode mode) override {
#if defined(__linux__)
        int flags = SYNC_FILE_RANGE_WRITE;
        if (mode == FLUSH_SYNC) {
            flags |= SYNC_FILE_RANGE_WAIT_BEFORE;
            flags |= SYNC_FILE_RANGE_WAIT_AFTER;
        }
        if (sync_file_range(_fd, 0, 0, flags) < 0) {
            return io_error(_filename, errno);
        }
        RETURN_IF_ERROR(_push_to_remote());
#else
        if (mode == FLUSH_SYNC && fsync(_fd) < 0) {
            return io_error(_filename, errno);
        }
#endif
        return Status::OK();
    }

    Status sync() override {
        if (_pending_sync) {
            _pending_sync = false;
            RETURN_IF_ERROR(do_sync(_fd, _filename));
        }
        return Status::OK();
    }

    uint64_t size() const override { return _filesize; }
    const string& filename() const override { return _filename; }

private:
    Status _push_to_remote();

    std::string _filename;
    int _fd;
    const bool _sync_on_close = false;
    bool _pending_sync = false;
    bool _closed = false;
    uint64_t _filesize = 0;
    uint64_t _pre_allocated_size = 0;
};

Status BosWritableFile::_push_to_remote() {
    bos_options.endpoint = "http://bj.bcebos.com";
    bossdk::Client client(AK, SK, bos_options);

    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        ENV_RETRUN_NOT_OK(client.upload_file(BUCKET, _filename, _filename));
    }
    DorisMetrics::blocks_push_remote_duration_us.increment(duration_ns >> 10);
    return Status::OK();
}

class BosEnv : public Env {
public:
    BosEnv(bossdk::Client* client) : _client(client) {}
    virtual ~BosEnv() {}

    Status new_sequential_file(const std::string& fname,
                               std::unique_ptr<SequentialFile>* result) override {
        return Status::NotSupported("Bos Env not support sequential file");
    }

    Status new_random_access_file(const std::string& fname,
                                  std::unique_ptr<RandomAccessFile>* result) override {
        return Status::NotSupported("Bos Env not support random access file");
    }
    Status new_random_access_file(const RandomAccessFileOptions& opts,
                                  const std::string& fname,
                                  std::unique_ptr<RandomAccessFile>* result) override {
        return Status::NotSupported("Bos Env not support random access file");
    }

    Status new_writable_file(const string& fname, std::unique_ptr<WritableFile>* result) override {
        return new_writable_file(WritableFileOptions(), fname, result);
    }
    Status new_writable_file(const WritableFileOptions& opts,
                             const string& fname,
                             std::unique_ptr<WritableFile>* result) override {
        int fd;
        RETURN_IF_ERROR(do_open(fname, opts.mode, &fd));

        uint64_t file_size = 0;
        if (opts.mode == MUST_EXIST) {
            RETURN_IF_ERROR(get_file_size(fname, &file_size));
        }
        result->reset(new BosWritableFile(fname, fd, file_size, opts.sync_on_close));
        return Status::OK();
    }

    Status new_random_rw_file(const std::string& fname,
                              std::unique_ptr<RandomRWFile>* result) override {
        return Status::NotSupported("Bos Env not support randome rw file");
    }
    Status new_random_rw_file(const RandomRWFileOptions& opts,
                              const std::string& fname,
                              std::unique_ptr<RandomRWFile>* result) override {
        return Status::NotSupported("Bos Env not support randome rw file");
    }

    Status path_exists(const std::string& fname) override {
        DCHECK(!fname.empty());
        bossdk::HeadObjectRequest req(_bucket_name, fname);
        bossdk::HeadObjectResponse resp;
        RETURN_IF_ERROR(_send_head_request(req, &resp));
        if (resp.status_code() == 404) {
            return Status::NotFound(strings::Substitute("path not exist. key=$0", fname));
        }
        return Status::OK();
    }

    Status get_children(const std::string& dir, std::vector<std::string>* result) override {
        return Status::NotSupported("Bos Env not support get_children. (TODO)");
    }

    Status iterate_dir(const std::string& dir,
                       const std::function<bool(const char*)>& cb) override {
        return Status::NotSupported("Bos Env not support iterate_dir. (TODO)");
    }

    Status delete_file(const std::string& fname) override {
        ENV_RETRUN_NOT_OK(_client->delete_object(_bucket_name, fname));
        return Status::OK();
    }

    // In Bos, A dir is a zero-size file with a name ending in '/'.
    Status create_dir(const std::string& name) override {
        DCHECK(!name.empty() && name.back() == '/');
        ENV_RETRUN_NOT_OK(_client->put_object(_bucket_name, name, ""));
        return Status::OK();
    }

    Status sync_dir(const string& dirname) override {
        return Status::NotSupported("Bos Env not support sync_dir" + dirname + ". (TODO)");
    }

    // should be exist, and file name ends with '/'
    // 1. If not exist, return error;
    // 2. If exist, return ok.
    //   2.1 is_dir param will be TRUE if it is a dir; FALSE if it is not.
    Status is_directory(const std::string& path, bool* is_dir) override {
        RETURN_IF_ERROR(path_exists(path));
        if (path.empty() || path.back() == '/') {
            *is_dir = false;
        } else {
            *is_dir = true;
        }
        return Status::OK();
    }

    Status canonicalize(const std::string& path, std::string* result) override {
        return Status::NotSupported("Bos Env not support canonicalize. (TODO)");
    }

    Status create_dir_if_missing(const std::string& name, bool* created = nullptr) override {
        return Status::NotSupported("Bos Env not support create_dir_if_missing. (TODO)");
    }

    // For BOS, it is same with delete_file()
    Status delete_dir(const std::string& dirname) override {
        return delete_file(dirname);
    }

    Status get_file_size(const string& fname, uint64_t* size) override {
        bossdk::HeadObjectRequest req(_bucket_name, fname);
        bossdk::HeadObjectResponse resp;
        RETURN_IF_ERROR(_send_head_request(req, &resp));

        *size = resp.meta().content_length();
        return Status::OK();
    }

    Status get_file_modified_time(const std::string& fname, uint64_t* file_mtime) override {
        bossdk::HeadObjectRequest req(_bucket_name, fname);
        bossdk::HeadObjectResponse resp;
        RETURN_IF_ERROR(_send_head_request(req, &resp));

        *file_mtime = resp.meta().last_modified();
        return Status::OK();
    }

    // NOTE: BOS does not support renaming operations, so we can only simulate by copying,
    // which will cause two problems:
    // 1. This operation is relatively inefficient;
    // 2. This operation is not atomic;
    // NOTE: currently, we only support renaming inside one bucket
    Status rename_file(const std::string& src_fname, const std::string& dest_fname) override {
        ENV_RETRUN_NOT_OK(_client->copy_object(
                    _bucket_name, src_fname, _bucket_name, dest_fname, "STANDARD"));
        return Status::OK();
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("Bos Env not support link_file. (TODO)");
    }

private:
    Status _send_head_request(bossdk::HeadObjectRequest& req, bossdk::HeadObjectResponse* resp) {
        ENV_RETRUN_NOT_OK(_client->head_object(req, resp));
        return Status::OK();
    }

    // TODO(lingbin): should make on progress only have one BosEnv object?
    // One possible solution: make the _bucket_name into filename, so we can
    // analyse the bucket_name from filename.
    //
    bossdk::Client* _client;
    std::string _bucket_name;
};

Env* Env::bos() {
    bos_options.endpoint = "http://bj.bcebos.com";
    bcesdk_ns::Client client(AK, SK, bos_options);
    static BosEnv bos_env(&client);
    return &bos_env;
}

} // end namespace doris
