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

#include "olap/memtable_flush_executor.h"

#include <functional>

#include "olap/memtable.h"
#include "util/scoped_cleanup.h"

namespace doris {

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat) {
    os << "(flush time(ms)=" << stat.flush_time_ns / 1000 / 1000
       << ", flush count=" << stat.flush_count << ")";
    return os;
}

OLAPStatus FlushToken::submit(std::shared_ptr<MemTable> memtable) {
    RETURN_NOT_OK(_flush_status.load());
    _flush_token->submit_func(std::bind(&FlushToken::_flush_memtable, this, memtable));
    return OLAP_SUCCESS;
}

void FlushToken::cancel() {
    _flush_token->shutdown();
}

OLAPStatus FlushToken::wait() {
    _flush_token->wait();
    return _flush_status.load();
}

void FlushToken::_flush_memtable(std::shared_ptr<MemTable> memtable) {
    SCOPED_CLEANUP({ memtable.reset(); });

    // If previous flush has failed, return directly
    if (_flush_status.load() != OLAP_SUCCESS) {
        return;
    }

    MonotonicStopWatch timer;
    timer.start();
    _flush_status.store(memtable->flush());
    if (_flush_status.load() != OLAP_SUCCESS) {
        return;
    }

    _stats.flush_time_ns += timer.elapsed_time();
    _stats.flush_count++;
    _stats.flush_size_bytes += memtable->memory_usage();
}

void MemTableFlushExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int32_t data_dir_num = data_dirs.size();
    size_t min_threads = std::max(1, config::flush_thread_num_per_store);
    size_t max_threads = data_dir_num * min_threads;
    ThreadPoolBuilder("MemTableFlushThreadPool")
            .set_min_threads(min_threads)
            .set_max_threads(max_threads)
            .build(&_flush_pool);
}

// NOTE: we use SERIAL mode here to ensure all mem-tables from one tablet are flushed in order.
OLAPStatus MemTableFlushExecutor::create_flush_token(std::unique_ptr<FlushToken>* flush_token) {
    flush_token->reset(new FlushToken(_flush_pool->new_token(ThreadPool::ExecutionMode::SERIAL)));
    return OLAP_SUCCESS;
}

} // namespace doris
