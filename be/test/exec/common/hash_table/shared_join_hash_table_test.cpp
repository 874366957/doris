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

#include "exec/common/hash_table/shared_join_hash_table.h"

#include <gtest/gtest.h>

#include <memory>

#include "exec/common/hash_table/hash_crc32_return32.h"

namespace doris {
TEST(SharedJoinHashTableTest, SerialSameBucket) {
    auto ht = std::make_shared<SharedJoinHashTable<UInt64, HashCRC32Return32<UInt64>>>();
    ht->prepare_build(4, 8);
    ASSERT_EQ(9U, ht->first.size());
    ASSERT_EQ(4U, ht->next.size());
    uint32_t buckets[4] {0, 2, 2, 2};
    ht->build_range<false>(buckets, 1, 4);
    ht->finish_build(false);
    EXPECT_EQ(3U, ht->first[2]);
    EXPECT_EQ(2U, ht->next[3]);
    EXPECT_EQ(1U, ht->next[2]);
    EXPECT_EQ(0U, ht->next[1]);
    EXPECT_EQ(0U, ht->next[0]);
}
} // namespace doris
