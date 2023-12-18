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

use databend_common_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_common_meta_stoerr::MetaStorageError;

/// Convert one line of serialized key-value into json.
///
/// Exported data is a pair of key with one char prefix and value.
/// Both key and value are in Vec<u8> format.
/// The prefix identifies the subtree this record belongs to. See [`SledKeySpace`].
///
/// The output json is in form of `(tree_name, {keyspace: {key, value}})`.
/// In this impl the `tree_name` can be one of `state`, `log` and `sm`. See [`MetaRaftStore`].
pub fn vec_kv_to_json(tree_name: &str, kv: &[Vec<u8>]) -> Result<String, MetaStorageError> {
    let kv_entry = RaftStoreEntry::deserialize(&kv[0], &kv[1])?;

    let tree_kv = (tree_name, kv_entry);
    let line = serde_json::to_string(&tree_kv)?;
    Ok(line)
}
