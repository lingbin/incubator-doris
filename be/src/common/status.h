// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>
#include <vector>

#include "common/logging.h"
#include "common/compiler_util.h"
#include "gen_cpp/Status_types.h"  // for TStatus
#include "gen_cpp/status.pb.h" // for PStatus
#include "util/slice.h" // for Slice

namespace doris {

class Status {
public:
    Status() : _state(nullptr) { }
    ~Status() noexcept { delete[] _state; }

    // Here _state == nullptr(Status::OK) is the most common case, so use this as
    // a fast path. This check can avoid one function call in most cases, because
    // _copy_state() is not an inline function.
    Status(const Status& s) : _state(s._state == nullptr ? nullptr : _copy_state(s._state))  { }

    // same as copy c'tor
    Status& operator=(const Status& s) {
        // The following condition catches both aliasing (when this == &s),
        // and the common case where both s and *this are OK.
        if (_state != s._state) {
            delete[] _state;
            _state = (s._state == nullptr) ? nullptr : _copy_state(s._state);
        }
        return *this;
    }

    Status(Status&& s) noexcept : _state(s._state) {
        s._state = nullptr;
    }

    Status& operator=(Status&& s) noexcept {
        if (_state != s._state) {
            delete[] _state;
            _state = s._state;
            s._state = nullptr;
        }
        return *this;
    }

    Status(const TStatus& t_status);
    Status(const PStatus& pstatus);

    static Status OK() { return Status(); }
    static Status Cancelled(const Slice& msg,
                            int16_t posix_code = -1,
                            const Slice& msg2 = Slice()) {
        return Status(TStatusCode::CANCELLED, msg, posix_code, msg2);
    }
    static Status NotSupported(const Slice& msg,
                               int16_t posix_code = -1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::NOT_IMPLEMENTED_ERROR, msg, posix_code, msg2);
    }
    static Status RuntimeError(const Slice& msg,
                               int16_t posix_code = -1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::RUNTIME_ERROR, msg, posix_code, msg2);
    }
    static Status MemoryLimitExceeded(const Slice& msg,
                                      int16_t posix_code = -1,
                                      const Slice& msg2 = Slice()) {
        return Status(TStatusCode::MEM_LIMIT_EXCEEDED, msg, posix_code, msg2);
    }
    static Status InternalError(const Slice& msg,
                               int16_t posix_code = -1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::INTERNAL_ERROR, msg, posix_code, msg2);
    }
    static Status ThriftRpcError(const Slice& msg,
                                 int16_t posix_code = -1,
                                 const Slice& msg2 = Slice()) {
        return Status(TStatusCode::THRIFT_RPC_ERROR, msg, posix_code, msg2);
    }
    static Status TimedOut(const Slice& msg,
                           int16_t posix_code = -1,
                           const Slice& msg2 = Slice()) {
        return Status(TStatusCode::TIMEOUT, msg, posix_code, msg2);
    }
    static Status MemoryAllocFailed(const Slice& msg,
                                    int16_t posix_code = -1,
                                    const Slice& msg2 = Slice()) {
        return Status(TStatusCode::MEM_ALLOC_FAILED, msg, posix_code, msg2);
    }
    static Status BufferAllocFailed(const Slice& msg,
                                    int16_t posix_code = -1,
                                    const Slice& msg2 = Slice()) {
        return Status(TStatusCode::BUFFER_ALLOCATION_FAILED, msg, posix_code, msg2);
    }
    static Status MinimumReservationUnavailable(const Slice& msg,
                                                int16_t posix_code = -1,
                                                const Slice& msg2 = Slice()) {
        return Status(TStatusCode::MINIMUM_RESERVATION_UNAVAILABLE, msg, posix_code, msg2);
    }
    static Status PublishTimeout(const Slice& msg,
                                 int16_t posix_code = 1,
                                 const Slice& msg2 = Slice()) {
        return Status(TStatusCode::PUBLISH_TIMEOUT, msg, posix_code, msg2);
    }
    static Status TooManyTasks(const Slice& msg,
                               int16_t posix_code = -1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::TOO_MANY_TASKS, msg, posix_code, msg2);
    }
    static Status EndOfFile(const Slice& msg,
                            int16_t posix_code = -1,
                            const Slice& msg2 = Slice()) {
        return Status(TStatusCode::END_OF_FILE, msg, posix_code, msg2);
    }
    static Status NotFound(const Slice& msg,
                           int16_t posix_code = -1,
                           const Slice& msg2 = Slice()) {
        return Status(TStatusCode::NOT_FOUND, msg, posix_code, msg2);
    }
    static Status Corruption(const Slice& msg,
                             int16_t posix_code = -1,
                             const Slice& msg2 = Slice()) {
        return Status(TStatusCode::CORRUPTION, msg, posix_code, msg2);
    }
    static Status InvalidArgument(const Slice& msg,
                                  int16_t posix_code = -1,
                                  const Slice& msg2 = Slice()) {
        return Status(TStatusCode::INVALID_ARGUMENT, msg, posix_code, msg2);
    }
    static Status IOError(const Slice& msg,
                          int16_t posix_code = -1,
                          const Slice& msg2 = Slice()) {
        return Status(TStatusCode::IO_ERROR, msg, posix_code, msg2);
    }
    static Status AlreadyExist(const Slice& msg,
                               int16_t posix_code = -1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::ALREADY_EXIST, msg, posix_code, msg2);
    }
    static Status Aborted(const Slice& msg,
                          int16_t posix_code = -1,
                          const Slice& msg2 = Slice()) {
        return Status(TStatusCode::ABORTED, msg, posix_code, msg2);
    }
    static Status ServiceUnavailable(const Slice& msg,
                                     int16_t posix_code = -1,
                                     const Slice& msg2 = Slice()) {
        return Status(TStatusCode::SERVICE_UNAVAILABLE, msg, posix_code, msg2);
    }
    static Status Uninitialized(const Slice& msg,
                                int16_t posix_code = -1,
                                const Slice& msg2 = Slice()) {
        return Status(TStatusCode::UNINITIALIZED, msg, posix_code, msg2);
    }

    bool ok() const { return _state == nullptr; }
    bool is_cancelled() const { return code() == TStatusCode::CANCELLED; }
    bool is_mem_limit_exceeded() const { return code() == TStatusCode::MEM_LIMIT_EXCEEDED; }
    bool is_thrift_rpc_error() const { return code() == TStatusCode::THRIFT_RPC_ERROR; }
    bool is_end_of_file() const { return code() == TStatusCode::END_OF_FILE; }
    bool is_not_found() const { return code() == TStatusCode::NOT_FOUND; }
    bool is_invalid_argument() const { return code() == TStatusCode::INVALID_ARGUMENT; }
    bool is_io_error() const {return code() == TStatusCode::IO_ERROR; }
    bool is_already_exist() const { return code() == TStatusCode::ALREADY_EXIST; }
    bool is_aborted() const { return code() == TStatusCode::ABORTED; }
    bool is_service_unavailable() const { return code() == TStatusCode::SERVICE_UNAVAILABLE; }
    bool is_uninitialized() const { return code() == TStatusCode::UNINITIALIZED; }

    // Convert into TStatus. Call this if 'status_container' contains an optional
    // TStatus field named 'status'. This also sets __isset.status.
    // NOTE: If the 'status' is required in Thrift Struct, this method can not
    // be used(use `to_thrift()` instead).
    template <typename T>
    void set_t_status(T* status_container) const {
        to_thrift(&status_container->status);
        status_container->__isset.status = true;
    }

    void to_thrift(TStatus* status) const;
    void to_protobuf(PStatus* status) const;

    std::string get_error_msg() const {
        return message().to_string();
    }

    /// @return A string representation of this status suitable for printing.
    ///   Returns the string "OK" for success.
    std::string to_string() const;

    /// @return A string representation of the status code, without the message
    ///   text or sub code information.
    std::string code_as_string() const;

    // This is similar to to_string, except that it does not include
    // the stringified error code or sub code.
    //
    // @note The returned Slice is only valid as long as this Status object
    //   remains live and unchanged.
    //
    // @return The message portion of the Status. For @c OK statuses,
    //   this returns an empty string.
    Slice message() const;

    TStatusCode::type code() const {
        return _state == nullptr ? TStatusCode::OK : static_cast<TStatusCode::type>(_state[4]);
    }

    int16_t posix_code() const {
        if (_state == nullptr) {
            return 0;
        }
        int16_t posix_code;
        memcpy(&posix_code, _state + 5, sizeof(posix_code));
        return posix_code;
    }

    /// Clone this status and add the specified prefix to the message.
    ///
    /// If this status is OK, then an OK status will be returned.
    ///
    /// @param [in] msg
    ///   The message to prepend.
    /// @return A new Status object with the same state plus an additional
    ///   leading message.
    Status clone_and_prepend(const Slice& msg) const;

    /// Clone this status and add the specified suffix to the message.
    ///
    /// If this status is OK, then an OK status will be returned.
    ///
    /// @param [in] msg
    ///   The message to append.
    /// @return A new Status object with the same state plus an additional
    ///   trailing message.
    Status clone_and_append(const Slice& msg) const;

private:
    const char* _copy_state(const char* state);

    Status(TStatusCode::type code, const Slice& msg, int16_t posix_code, const Slice& msg2);

private:
    // OK status has a nullptr _state.  Otherwise, _state is a new[] array
    // of the following form:
    //    _state[0..3] == length of message
    //    _state[4]    == code
    //    _state[5..6] == posix_code
    //    _state[7..]  == message
    const char* _state;
};

// some generally useful macros
#define RETURN_IF_ERROR(stmt)            \
    do {                                 \
        const Status& _status_ = (stmt); \
        if (UNLIKELY(!_status_.ok())) {  \
            return _status_;             \
        }                                \
    } while (false)

#define EXIT_IF_ERROR(stmt)                         \
    do {                                            \
        const Status& _status_ = (stmt);            \
        if (UNLIKELY(!_status_.ok())) {             \
            string msg = _status_.to_string();      \
            LOG(ERROR) << msg;                      \
            exit(1);                                \
        }                                           \
    } while (false)

/// @brief Emit a warning if @c to_call returns a bad status.
#define WARN_IF_ERROR(to_call, warning_prefix)                          \
    do {                                                                \
        const Status& _s = (to_call);                                   \
        if (UNLIKELY(!_s.ok())) {                                       \
            LOG(WARNING) << (warning_prefix) << ": " << _s.to_string(); \
        }                                                               \
    } while (false);

#define RETURN_WITH_WARN_IF_ERROR(stmt, ret_code, warning_prefix)              \
    do {                                                                       \
        const Status& _s = (stmt);                                             \
        if (UNLIKELY(!_s.ok())) {                                              \
            LOG(WARNING) << (warning_prefix) << ", error: " << _s.to_string(); \
            return ret_code;                                                   \
        }                                                                      \
    } while (false);

// Return the given status if it is not OK, but first clone it and prepend the given message.
#define RETURN_NOT_OK_PREPEND(s, msg)         \
    do {                                      \
        const Status& _s = (s);               \
        if (UNLIKELY(!_s.ok())) {             \
            return _s.clone_and_prepend(msg); \
        }                                     \
    } while (0)

} // end namespace doris

#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
