// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod hashjoin_hashtable;
mod hashjoin_string_hashtable;
mod traits;

pub use databend_common_hashtable::RowPtr;
pub use hashjoin_hashtable::HashJoinHashTable;
pub use hashjoin_hashtable::RawEntry;
pub use hashjoin_hashtable::combine_header;
pub use hashjoin_hashtable::early_filtering;
pub use hashjoin_hashtable::hash_bits;
pub use hashjoin_hashtable::new_header;
pub use hashjoin_hashtable::remove_header_tag;
pub use hashjoin_hashtable::tag;
pub use hashjoin_string_hashtable::HashJoinStringHashTable;
pub use hashjoin_string_hashtable::STRING_EARLY_SIZE;
pub use hashjoin_string_hashtable::StringRawEntry;
pub use traits::HashJoinHashtableLike;

pub type HashJoinHashMap<K, const UNIQUE: bool = false> = HashJoinHashTable<K, UNIQUE>;
pub type BinaryHashJoinHashMap<const UNIQUE: bool = false> = HashJoinStringHashTable<UNIQUE>;
