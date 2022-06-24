// Copyright 2021 Datafuse Labs.
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

use common_meta_raft_store::sled_key_spaces;
use common_meta_raft_store::sled_key_spaces::KeySpaceKV;
use common_meta_sled_store::SledKeySpace;
use common_meta_sled_store::SledOrderedSerde;
use common_meta_sled_store::SledSerde;
use common_meta_types::MetaStorageError;

/// Convert (sub_tree_prefix, key, value, key_space1, key_space2...) into a [`KeySpaceKV`] for export.
macro_rules! to_kv_variant {
    ($prefix: expr, $vec_key: expr, $vec_value: expr, $($key_space: tt),+ ) => {
        $(

        if <sled_key_spaces::$key_space as SledKeySpace>::PREFIX == $prefix {
            let key = <<sled_key_spaces::$key_space as SledKeySpace>::K as SledOrderedSerde>::de(
                $vec_key,
            )?;
            let val =
                <<sled_key_spaces::$key_space as SledKeySpace>::V as SledSerde>::de($vec_value)?;
            return Ok(KeySpaceKV::$key_space { key, value: val, });
        }
        )+
    };
}

pub fn serialize_kv_variant(kv: &KeySpaceKV) -> Result<(sled::IVec, sled::IVec), MetaStorageError> {
    macro_rules! ser {
        ($ks:tt, $key:expr, $value:expr) => {
            Ok((
                sled_key_spaces::$ks::serialize_key($key)?,
                sled_key_spaces::$ks::serialize_value($value)?,
            ))
        };
    }

    match kv {
        KeySpaceKV::Logs { key, value } => ser!(Logs, key, value),
        KeySpaceKV::Nodes { key, value } => ser!(Nodes, key, value),
        KeySpaceKV::StateMachineMeta { key, value } => ser!(StateMachineMeta, key, value),
        KeySpaceKV::RaftStateKV { key, value } => ser!(RaftStateKV, key, value),
        KeySpaceKV::GenericKV { key, value } => ser!(GenericKV, key, value),
        KeySpaceKV::Sequences { key, value } => ser!(Sequences, key, value),
        KeySpaceKV::ClientLastResps { key, value } => ser!(ClientLastResps, key, value),
        KeySpaceKV::LogMeta { key, value } => ser!(LogMeta, key, value),
    }
}

pub fn deserialize_to_kv_variant(d: &[Vec<u8>]) -> Result<KeySpaceKV, MetaStorageError> {
    let prefix_key = &d[0];
    let vec_value = &d[1];

    let prefix = prefix_key[0];
    let vec_key = &prefix_key[1..];

    to_kv_variant!(
        prefix,
        vec_key,
        vec_value,
        // Avaliable key spaces:
        Logs,
        Nodes,
        StateMachineMeta,
        RaftStateKV,
        GenericKV,
        Sequences,
        ClientLastResps,
        LogMeta
    );

    unreachable!("unknown prefix: {}", prefix);
}

/// Convert one line of serialized key-value into json.
///
/// Exported data is a pair of key with one char prefix and value.
/// Both key and value are in Vec<u8> format.
/// The prefix identifies the subtree this record belongs to. See [`SledKeySpace`].
///
/// The output json is in form of `(tree_name, {keyspace: {key, value}})`.
/// In this impl the `tree_name` can be one of `state`, `log` and `sm`. See [`MetaRaftStore`].
pub fn vec_kv_to_json(tree_name: &str, kv: &[Vec<u8>]) -> Result<String, MetaStorageError> {
    let kv_variant = deserialize_to_kv_variant(kv)?;

    let tree_kv = (tree_name, kv_variant);
    let line = serde_json::to_string(&tree_kv)?;
    Ok(line)
}
