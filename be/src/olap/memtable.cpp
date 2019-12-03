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

#include "olap/memtable.h"

#include "common/object_pool.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/row_cursor.h"
#include "olap/row.h"
#include "olap/schema.h"
#include "runtime/tuple.h"
#include "util/debug_util.h"
#include "util/logging.h"
#include "util/runtime_profile.h"

namespace doris {

MemTable::MemTable(int64_t tablet_id, Schema* schema, const TabletSchema* tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   KeysType keys_type, RowsetWriter* rowset_writer, MemTracker* mem_tracker)
    : _tablet_id(tablet_id),
      _schema(schema),
      _tablet_schema(tablet_schema),
      _tuple_desc(tuple_desc),
      _slot_descs(slot_descs),
      _keys_type(keys_type),
      _row_comparator(_schema),
      _rowset_writer(rowset_writer) {

    _schema_size = _schema->schema_size();
    _mem_tracker.reset(new MemTracker(-1, "memtable", mem_tracker));
    _tmp_mem_pool.reset(new MemPool(_mem_tracker.get()));
    _table_mem_pool.reset(new MemPool(_mem_tracker.get()));
    _skip_list = new Table(_row_comparator, _table_mem_pool.get(), _keys_type == KeysType::DUP_KEYS);
}

MemTable::~MemTable() {
    delete _skip_list;
}

MemTable::RowCursorComparator::RowCursorComparator(const Schema* schema)
    : _schema(schema) {}

int MemTable::RowCursorComparator::operator()(const char* left, const char* right) const {
    ContiguousRow lhs_row(_schema, left);
    ContiguousRow rhs_row(_schema, right);
    return compare_row(lhs_row, rhs_row);
}

void MemTable::insert(const Tuple* tuple) {
    bool overwritten = false;
    if (_keys_type == KeysType::DUP_KEYS) {
        // Will insert directly, so use memory from _table_mem_pool
        _tuple_buf = _table_mem_pool->allocate(_schema_size);
        ContiguousRow row(_schema, _tuple_buf);
        _tuple_to_row(tuple, &row, _table_mem_pool.get());
        _skip_list->Insert((char*)_tuple_buf, &overwritten);
        DCHECK(!overwritten) << "Duplicate key model meet overwrite in SkipList";
        return;
    }

    _tuple_buf = _tmp_mem_pool->allocate(_schema_size);
    ContiguousRow row(_schema, _tuple_buf);
    _tuple_to_row(tuple, &row, _table_mem_pool.get());

    // TODO(lingbin): Remove redundant contain check
    if (_skip_list->Contains((char*)_tuple_buf)) {
        // Will aggregate, use memory from _tmp_mem_pool
        _skip_list->Insert((char*)_tuple_buf, &overwritten);
        DCHECK(overwritten) << "Does not meet duplicated key in SkipList";
    } else {
        // Will insert directly, so use memory from _table_mem_pool
        _tuple_buf = _table_mem_pool->allocate(_schema_size);
        ContiguousRow dst_row(_schema, _tuple_buf);
        copy_row(&dst_row, row, _table_mem_pool.get());
        _skip_list->Insert((char*)_tuple_buf, &overwritten);
        DCHECK(!overwritten) << "Meet unexpected duplicated key in SkipList";
    }

    // Make MemPool to be reusable, but does not free its memory
    _tmp_mem_pool->clear();
}

void MemTable::_tuple_to_row(const Tuple* tuple, ContiguousRow* row, MemPool* mem_pool) {
    for (size_t i = 0; i < _slot_descs->size(); ++i) {
        auto cell = row->cell(i);
        const SlotDescriptor* slot = (*_slot_descs)[i];

        bool is_null = tuple->is_null(slot->null_indicator_offset());
        const void* value = tuple->get_slot(slot->tuple_offset());
        _schema->column(i)->consume(
                &cell, (const char*)value, is_null, _tmp_mem_pool.get(), &_agg_object_pool);
    }
}

OLAPStatus MemTable::flush() {
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        Table::Iterator it(_skip_list);
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            char* row = (char*)it.key();
            ContiguousRow dst_row(_schema, row);
            agg_finalize_row(&dst_row, _table_mem_pool.get());
            RETURN_NOT_OK(_rowset_writer->add_row(dst_row));
        }
        RETURN_NOT_OK(_rowset_writer->flush());
    }
    DorisMetrics::memtable_flush_total.increment(1);
    DorisMetrics::memtable_flush_duration_us.increment(duration_ns / 1000);
    return OLAP_SUCCESS;
}

OLAPStatus MemTable::close() {
    return flush();
}

} // namespace doris
