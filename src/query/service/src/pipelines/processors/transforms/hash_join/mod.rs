// Copyright 2022 Datafuse Labs.
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

mod common;
mod desc;
mod hash_join_state;
mod hash_join_state_impl;
mod join_hash_table;
mod probe_join;
mod probe_state;
mod result_blocks;
pub(crate) mod row;
mod util;

pub use desc::HashJoinDesc;
pub use hash_join_state::HashJoinState;
pub use join_hash_table::FixedKeyHashTable;
pub use join_hash_table::HashTable;
pub use join_hash_table::JoinHashTable;
pub type KeyU128HashTable = FixedKeyHashTable<u128>;
pub type KeyU16HashTable = FixedKeyHashTable<u16>;
pub type KeyU256HashTable = FixedKeyHashTable<primitive_types::U256>;
pub type KeyU32HashTable = FixedKeyHashTable<u32>;
pub type KeyU512HashTable = FixedKeyHashTable<primitive_types::U512>;
pub type KeyU64HashTable = FixedKeyHashTable<u64>;
pub type KeyU8HashTable = FixedKeyHashTable<u8>;
pub use join_hash_table::SerializerHashTable;
pub use probe_state::ProbeState;
pub use result_blocks::*;
