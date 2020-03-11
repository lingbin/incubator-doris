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

#include "olap/fs/bos_block_manager.h"

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <numeric>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>

#include <common/logging.h>
#include "env/env.h"
#include "env/env_util.h"
#include "olap/fs/block_id.h"
#include "olap/fs/block_manager_metrics.h"
#include "gutil/integral_types.h"
#include "gutil/port.h"
#include "gutil/stringprintf.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem_tracker.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/path_util.h"
#include "util/slice.h"

using std::accumulate;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace doris {
namespace fs {

namespace internal {

////////////////////////////////////////////////////////////
// BosWritableBlock
////////////////////////////////////////////////////////////

// A bos-backed block that has been opened for writing.
//
// Contains a pointer to the block manager as well as file path
// so that dirty metadata can be synced via BlockManager::SyncMetadata()
// at Close() time. Embedding a file path (and not a simpler
// BlockId) consumes more memory, but the number of outstanding
// BosWritableBlock instances is expected to be low.
class BosWritableBlock : public WritableBlock {
public:
    BosWritableBlock(BosBlockManager* block_manager,
                     string path,
                     shared_ptr<WritableFile> writer);
    virtual ~BosWritableBlock();

    virtual Status close() override;
    virtual Status abort() override;

    virtual BlockManager* block_manager() const override;
    virtual const BlockId& id() const override;
    virtual const std::string& path() const override;

    virtual Status append(const Slice& data) override;
    virtual Status appendv(const Slice* data, size_t data_cnt) override;

    virtual Status finalize() override;
    virtual size_t bytes_appended() const override;

    virtual State state() const override;

    void handle_error(const Status& s) const;

    // Starts an asynchronous flush of dirty block data to disk.
    Status flush_data_async();

private:
    enum SyncMode {
        SYNC,
        NO_SYNC
    };

    // Close the block, optionally synchronizing dirty data and metadata.
    Status _close(SyncMode mode);

    // Back pointer to the block manager.
    //
    // Should remain alive for the lifetime of this block.
    BosBlockManager* block_manager_;

    const BlockId block_id_;
    const string _path;

    // The underlying opened file backing this block.
    shared_ptr<WritableFile> writer_;

    State state_;

    // The number of bytes successfully appended to the block.
    size_t bytes_appended_;

    DISALLOW_COPY_AND_ASSIGN(BosWritableBlock);
};

BosWritableBlock::BosWritableBlock(BosBlockManager* block_manager,
                                   string path,
                                   shared_ptr<WritableFile> writer) :
        block_manager_(block_manager),
        _path(std::move(path)),
        writer_(std::move(writer)),
        state_(CLEAN),
        bytes_appended_(0) {
    if (block_manager_->metrics_) {
        block_manager_->metrics_->blocks_open_writing->increment(1);
        block_manager_->metrics_->total_writable_blocks->increment(1);
    }
}

BosWritableBlock::~BosWritableBlock() {
    if (state_ != CLOSED) {
        WARN_IF_ERROR(abort(), Substitute("Failed to close block $0", _path));
    }
}

// void BosWritableBlock::handle_error(const Status& s) const {
//     HANDLE_DISK_FAILURE(
//         s, block_manager_->error_manager()->RunErrorNotificationCb(
//             ErrorHandlerType::DISK_ERROR, location_.data_dir()));
// }

Status BosWritableBlock::close() {
    return _close(SYNC);
}

Status BosWritableBlock::abort() {
    RETURN_IF_ERROR(_close(NO_SYNC));
    return block_manager_->_delete_block(_path);
}

BlockManager* BosWritableBlock::block_manager() const {
    return block_manager_;
}

const BlockId& BosWritableBlock::id() const {
    CHECK(false) << "Not support Block.id(). (TODO)";
    return block_id_;
}

const string& BosWritableBlock::path() const {
    return _path;
}

Status BosWritableBlock::append(const Slice& data) {
    return appendv(&data, 1);
}

Status BosWritableBlock::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(state_ == CLEAN || state_ == DIRTY)
            << "path=" << _path
            << " invalid state=" << state_;
    RETURN_IF_ERROR(writer_->appendv(data, data_cnt));
    state_ = DIRTY;

    // Calculate the amount of data written
    size_t bytes_written = accumulate(data, data + data_cnt, static_cast<size_t>(0),
            [&](int sum, const Slice& curr) {
                return sum + curr.size;
            });
    bytes_appended_ += bytes_written;
    return Status::OK();
}

Status BosWritableBlock::flush_data_async() {
    VLOG(3) << "Flushing block " << _path;
    RETURN_IF_ERROR(writer_->flush(WritableFile::FLUSH_ASYNC));
    return Status::OK();
}

// TODO(lingbin): we can async push to remote from now.
Status BosWritableBlock::finalize() {
    DCHECK(state_ == CLEAN || state_ == DIRTY || state_ == FINALIZED)
            << "path=" << _path
            << "Invalid state: " << state_;

    if (state_ == FINALIZED) {
        return Status::OK();
    }
    VLOG(3) << "Finalizing block " << _path;
    if (state_ == DIRTY && BlockManager::block_manager_preflush_control == "finalize") {
        flush_data_async();
    }
    state_ = FINALIZED;
    return Status::OK();
}

size_t BosWritableBlock::bytes_appended() const {
    return bytes_appended_;
}

WritableBlock::State BosWritableBlock::state() const {
    return state_;
}

Status BosWritableBlock::_close(SyncMode mode) {
    if (state_ == CLOSED) {
        return Status::OK();
    }

    Status sync_st;
    if (mode == SYNC && (state_ == CLEAN || state_ == DIRTY || state_ == FINALIZED)) {
        // Safer to synchronize data first, then metadata.
        VLOG(3) << "Syncing block " << _path;
        if (block_manager_->metrics_) {
            block_manager_->metrics_->total_disk_sync->increment(1);
        }
        sync_st = writer_->sync();
        // if (sync_st.ok()) {
        //     sync_st = block_manager_->_sync_metadata(_path);
        // }
        WARN_IF_ERROR(sync_st, Substitute("Failed to sync when closing block $0", _path));
    }
    Status close_st = writer_->close();

    state_ = CLOSED;
    writer_.reset();
    if (block_manager_->metrics_) {
        block_manager_->metrics_->blocks_open_writing->increment(-1);
        block_manager_->metrics_->total_bytes_written->increment(bytes_appended_);
        block_manager_->metrics_->total_blocks_created->increment(1);
    }

    // Either Close() or Sync() could have run into an error.
    RETURN_IF_ERROR(close_st);
    RETURN_IF_ERROR(sync_st);

    // Prefer the result of Close() to that of Sync().
    return close_st.ok() ? close_st : sync_st;
}

////////////////////////////////////////////////////////////
// BosReadableBlock
////////////////////////////////////////////////////////////

// A file-backed block that has been opened for reading.
//
// There may be millions of instances of BosReadableBlock outstanding, so
// great care must be taken to reduce its size. To that end, it does _not_
// embed a FileBlockLocation, using the simpler BlockId instead.
class BosReadableBlock : public ReadableBlock {
public:
    BosReadableBlock(BosBlockManager* block_manager,
                      string path,
                      shared_ptr<RandomAccessFile> reader);

    virtual ~BosReadableBlock();

    virtual Status close() override;

    virtual BlockManager* block_manager() const override;

    virtual const BlockId& id() const override;
    virtual const std::string& path() const override;

    virtual Status size(uint64_t* sz) const override;

    virtual Status read(uint64_t offset, Slice result) const override;

    virtual Status readv(uint64_t offset, const Slice* results, size_t res_cnt) const override;

    void handle_error(const Status& s) const;

private:
    // Back pointer to the owning block manager.
    BosBlockManager* block_manager_;

    // The block's identifier.
    const BlockId block_id_;
    const string _path;

    // The underlying opened file backing this block.
    shared_ptr<RandomAccessFile> reader_;

    // Whether or not this block has been closed. Close() is thread-safe, so
    // this must be an atomic primitive.
    std::atomic_bool closed_;

    DISALLOW_COPY_AND_ASSIGN(BosReadableBlock);
};

BosReadableBlock::BosReadableBlock(BosBlockManager* block_manager,
                                     string path,
                                     shared_ptr<RandomAccessFile> reader) :
        block_manager_(block_manager),
        _path(std::move(path)),
        reader_(std::move(reader)),
        closed_(false) {
    if (block_manager_->metrics_) {
        block_manager_->metrics_->blocks_open_reading->increment(1);
        block_manager_->metrics_->total_readable_blocks->increment(1);
    }
}

BosReadableBlock::~BosReadableBlock() {
    WARN_IF_ERROR(close(), Substitute("Failed to close block $0", _path));
}

Status BosReadableBlock::close() {
    bool expected = false;
    if (closed_.compare_exchange_strong(expected, true)) {
        reader_.reset();
        if (block_manager_->metrics_) {
            block_manager_->metrics_->blocks_open_reading->increment(-1);
        }
    }

    return Status::OK();
}

BlockManager* BosReadableBlock::block_manager() const {
    return block_manager_;
}

const BlockId& BosReadableBlock::id() const {
    CHECK(false) << "Not support Block.id(). (TODO)";
    return block_id_;
}

const string& BosReadableBlock::path() const {
    return _path;
}

Status BosReadableBlock::size(uint64_t* sz) const {
    DCHECK(!closed_.load());

    RETURN_IF_ERROR(reader_->size(sz));
    return Status::OK();
}

Status BosReadableBlock::read(uint64_t offset, Slice result) const {
    return readv(offset, &result, 1);
}

Status BosReadableBlock::readv(uint64_t offset, const Slice* results, size_t res_cnt) const {
    DCHECK(!closed_.load());

    RETURN_IF_ERROR(reader_->readv_at(offset, results, res_cnt));

    if (block_manager_->metrics_) {
        // Calculate the read amount of data
        size_t bytes_read = accumulate(results, results + res_cnt, static_cast<size_t>(0),
                [&](int sum, const Slice& curr) {
                    return sum + curr.size;
                });
        block_manager_->metrics_->total_bytes_read->increment(bytes_read);
    }

    return Status::OK();
}

} // namespace internal

////////////////////////////////////////////////////////////
// BosBlockManager
////////////////////////////////////////////////////////////

BosBlockManager::BosBlockManager(Env* env, BlockManagerOptions opts) :
        env_(DCHECK_NOTNULL(env)),
        opts_(std::move(opts)),
        _mem_tracker(new MemTracker(-1, "file_block_manager", opts_.parent_mem_tracker.get())) {
    if (opts_.enable_metric) {
        metrics_.reset(new internal::BlockManagerMetrics());
    }
}

BosBlockManager::~BosBlockManager() {
}

Status BosBlockManager::open() {
    // TODO(lingbin)
    return Status::NotSupported("to be implemented. (TODO)");
}

Status BosBlockManager::create_block(const CreateBlockOptions& opts,
                                     unique_ptr<WritableBlock>* block) {
    CHECK(!opts_.read_only);

    shared_ptr<WritableFile> writer;
    WritableFileOptions wr_opts;
    wr_opts.sync_on_close = true;
    wr_opts.mode = Env::MUST_CREATE;
    RETURN_IF_ERROR(env_util::open_file_for_write(wr_opts, env_, opts.path, &writer));

    VLOG(1) << "Creating new block at " << opts.path;
    block->reset(new internal::BosWritableBlock(this, opts.path, writer));
    return Status::OK();
}

Status BosBlockManager::open_block(const std::string& path, unique_ptr<ReadableBlock>* block) {
    VLOG(1) << "Opening block with path at " << path;
    shared_ptr<RandomAccessFile> reader;
    RETURN_IF_ERROR(env_util::open_file_for_random(env_, path, &reader));
    block->reset(new internal::BosReadableBlock(this, path, reader));
    return Status::OK();
}

// TODO(lingbin): We should do something to ensure that deletion can only be done
// after the last reader or writer has finished
Status BosBlockManager::_delete_block(const string& path) {
  CHECK(!opts_.read_only);

  RETURN_IF_ERROR(env_->delete_file(path));

  // We don't bother fsyncing the parent directory as there's nothing to be
  // gained by ensuring that the deletion is made durable. Even if we did
  // fsync it, we'd need to account for garbage at startup time (in the
  // event that we crashed just before the fsync), and with such accounting
  // fsync-as-you-delete is unnecessary.
  //
  // The block's directory hierarchy is left behind. We could prune it if
  // it's empty, but that's racy and leaving it isn't much overhead.

  return Status::OK();
}

// TODO(lingbin): only one level is enough?
Status BosBlockManager::_sync_metadata(const string& path) {
    string dir = path_util::dir_name(path);
    if (metrics_) {
        metrics_->total_disk_sync->increment(1);
    }
    RETURN_IF_ERROR(env_->sync_dir(dir));
    return Status::OK();
}

// // TODO(lingbin): use a more general way
// BlockManager* BlockManager::bos_block_mgr() {
//   BlockManagerOptions opts;
//   opts.read_only = false;
//   Env* env = Env::bos();
//   return new BosBlockManager(env, opts);
// }

} // namespace fs
} // namespace kudu
