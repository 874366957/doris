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

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "common/check.h"
#include "common/logging.h"
#include "core/custom_allocator.h"

namespace doris {
class ProbeState {
private:
    uint32_t build_row;
    uint32_t probe_row;

public:
    ProbeState() {
        this->build_row = 0;
        this->probe_row = 0;
    }

    void reset() {
        this->build_row = 0;
        this->probe_row = 0;
    }

    bool finished(const uint32_t& probe_rows) const { return this->probe_row == probe_rows; }

    void set_build_row(const uint32_t& build_row) { this->build_row = build_row; }

    void set_probe_row(const uint32_t& probe_row) { this->probe_row = probe_row; }

    uint32_t& get_build_row() { return this->build_row; }

    uint32_t& get_probe_row() { return this->probe_row; }
};

// Row-index hash table for parallel construction and subsequent read-only chain access.
// The caller owns the build rows and keys, and computes bucket numbers.
// Call prepare_build() before use. Row 0 is a dummy row and the chain terminator.
// Synchronize completion of all builders, then call finish_build() before any chain reads.
template <typename Key, typename Hash>
class SharedJoinHashTable {
private:
    // Ordinary bucket count; first[bucket_size] is the extra NULL bucket.
    uint32_t bucket_size {1};
    // Total build rows, including the dummy row at index 0.
    uint32_t num_elem {1};
    // Bucket head row ids; 0 means the bucket is empty.
    DorisVector<uint32_t> first {0};
    // Links indexed by global build-row id; 0 marks the end of a chain.
    DorisVector<uint32_t> next {0};
    const Key* build_keys = nullptr;

public:
    using key_type = Key;

    // num_elem includes the dummy row; bucket_size excludes the extra NULL slot.
    // Finish all previous builders and readers before preparing a new build.
    void prepare_build(const uint32_t num_elem, const uint32_t bucket_size) {
        DORIS_CHECK(num_elem >= 1);
        DORIS_CHECK(bucket_size >= 1);
        // Insertion reads the old bucket heads, so clear them on every build.
        this->first.assign(static_cast<std::size_t>(bucket_size) + 1, 0);
        // Each real row's link is overwritten by build_range(); row 0 stays zero.
        this->next.resize(num_elem);
        this->bucket_size = bucket_size;
        this->num_elem = num_elem;
        this->build_keys = nullptr;
    }

    void begin_build(const Key* keys) { this->build_keys = keys; }

    const Key* get_build_keys() const { return this->build_keys; }

    // Insert disjoint global row ranges [begin, end), covering [1, num_elem) exactly once.
    // All workers in a concurrent build must use <true>. Keep bucket_nums valid and read-only,
    // and do not resize arrays or change metadata while builders run.
    // The caller must synchronize completion of every builder's entire loop before reading chains.
    template <bool is_concurrent>
    void build_range(const uint32_t* bucket_nums, uint32_t begin, uint32_t end) {
        DCHECK(begin >= 1 && begin <= end && end <= this->num_elem);
        for (uint32_t i = begin; i < end; i++) {
            uint32_t bucket_num = bucket_nums[i];
            DCHECK_LE(bucket_num, this->bucket_size);
            if constexpr (is_concurrent) {
                static_assert(std::atomic_ref<uint32_t>::required_alignment <= alignof(uint32_t));
                std::atomic_ref<uint32_t> head(first[bucket_num]);
                // The exchange may expose i before next[i] is written; it is not a ready signal.
                // The caller's completion barrier makes all links visible before chain reads.
                auto old_head = head.exchange(i, std::memory_order_relaxed);
                this->next[i] = old_head;
            } else {
                this->next[i] = this->first[bucket_num];
                this->first[bucket_num] = i;
            }
        }
    }

    // Call once after synchronizing every builder's entire loop, before exposing chains to readers.
    // This applies the NULL-bucket policy; it does not wait or publish readiness.
    void finish_build(bool keep_null_key) {
        if (!keep_null_key) {
            this->first[bucket_size] = 0;
        }
    }

    // Replace task-local probe bucket numbers with candidate build-row ids in place.
    // Input must contain bucket numbers, not row ids produced by an earlier call.
    // Call after finish_build(); candidates still require key comparison.
    void pre_build_idxs(DorisVector<uint32_t>& buckets) const {
        for (auto& bucket : buckets) {
            DCHECK_LE(bucket, this->bucket_size);
            bucket = this->first[bucket];
        }
    }

    uint32_t find_match(const Key& probe_key, uint32_t start_row) const {
        DCHECK(build_keys != nullptr);
        while (start_row != 0) {
            DCHECK_LT(start_row, this->num_elem);
            if (build_keys[start_row] == probe_key) {
                return start_row;
            }
            start_row = this->next[start_row];
        }
        return 0;
    }

    uint32_t probe_batch(const Key* probe_keys, const uint32_t* probe_heads, uint32_t probe_rows,
                         ProbeState& state, uint32_t* probe_idxs, uint32_t* build_idxs,
                         uint32_t capacity) const {
        DCHECK_GT(capacity, 0U);
        uint32_t res_row = 0;
        while (state.get_probe_row() < probe_rows) {
            uint32_t build_row = state.get_build_row() == 0 ? probe_heads[state.get_probe_row()]
                                                            : state.get_build_row();
            while (build_row != 0) {
                if (probe_keys[state.get_probe_row()] == build_keys[build_row]) {
                    probe_idxs[res_row] = state.get_probe_row();
                    build_idxs[res_row] = build_row;
                    res_row++;
                }
                build_row = next[build_row];
                if (res_row == capacity) {
                    state.get_build_row() = build_row;
                    // A finished chain must advance before yielding, or the next call
                    // would reload this probe row's head and emit its matches again.
                    if (build_row == 0) {
                        ++state.get_probe_row();
                    }
                    return res_row;
                }
            }
            state.get_probe_row()++;
            state.get_build_row() = build_row;
        }
        return res_row;
    }

    // These vector references do not extend the table's lifetime or synchronize reads.
    const DorisVector<uint32_t>& get_first() const { return this->first; }

    const DorisVector<uint32_t>& get_next() const { return this->next; }

    uint32_t get_bucket_size() const { return this->bucket_size; }

    // Total row slots, including the dummy row at index 0.
    std::size_t size() const { return num_elem; }

    // A single dummy row represents an empty build input.
    bool empty_build_side() const { return num_elem <= 1; }
};
} // namespace doris
