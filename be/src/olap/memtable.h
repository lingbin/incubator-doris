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

#ifndef DORIS_BE_SRC_OLAP_MEMTABLE_H
#define DORIS_BE_SRC_OLAP_MEMTABLE_H

#include "olap/skiplist.h"

namespace doris {

class RowsetWriter;
class ObjectPool;
class Schema;
class Tuple;

class MemTable {
public:
    MemTable(int64_t tablet_id, Schema* schema, const TabletSchema* tablet_schema,
             const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
             KeysType keys_type, RowsetWriter* rowset_writer, MemTracker* mem_tracker);
    ~MemTable();

    int64_t tablet_id() { return _tablet_id; }
    size_t memory_usage() {
        return _mem_tracker->consumption();
    }
    void insert(const Tuple* tuple);
    OLAPStatus flush();
    OLAPStatus close();

private:
    void _tuple_to_row(const Tuple* tuple, ContiguousRow* row, MemPool* mem_pool);

    int64_t _tablet_id;
    Schema* _schema;
    const TabletSchema* _tablet_schema;
    TupleDescriptor* _tuple_desc;
    // the slot in _slot_descs are in order of tablet's schema
    const std::vector<SlotDescriptor*>* _slot_descs;
    KeysType _keys_type;

    struct RowCursorComparator {
        const Schema* _schema;
        RowCursorComparator(const Schema* schema);
        int operator()(const char* left, const char* right) const;
    };

    RowCursorComparator _row_comparator;
    std::unique_ptr<MemTracker> _mem_tracker;
    // This is a buffer, to hold the memory referenced by the rows that have not
    // been inserted into the SkipList
    std::unique_ptr<MemPool> _tmp_mem_pool;
    std::unique_ptr<MemPool> _table_mem_pool;
    ObjectPool _agg_object_pool;

    typedef SkipList<char*, RowCursorComparator> Table;
    u_int8_t* _tuple_buf;
    size_t _schema_size;
    Table* _skip_list;

    RowsetWriter* _rowset_writer;

}; // class MemTable

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_MEMTABLE_H
