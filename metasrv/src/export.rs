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

use common_exception::ErrorCode;
use common_meta_raft_store::sled_key_spaces as sledks;
use common_meta_sled_store::SledKeySpace;
use common_meta_sled_store::SledOrderedSerde;
use common_meta_sled_store::SledSerde;

/// Convert (tree_name, sub_tree_prefix, key, value) into a one line json for export.
macro_rules! to_line {
    ($key_space: path, $typ: expr, $prefix: expr, $vec_key: expr, $vec_value: expr) => {
        if <$key_space as SledKeySpace>::PREFIX == $prefix {
            let key = <<$key_space as SledKeySpace>::K as SledOrderedSerde>::de($vec_key)?;
            let val = <<$key_space as SledKeySpace>::V as SledSerde>::de($vec_value)?;

            let line = serde_json::to_string(&($typ, $prefix, stringify!($key_space), key, val))?;
            return Ok(line);
        }
    };
}

/// Convert one line of exported data into json.
///
/// Exported data is a pair of key with one char prefix and value.
/// Both key and value are in Vec<u8> format.
/// The prefix identifies the subtree this record belongs to. See [`SledKeySpace`].
///
/// The output json is in form of `(tree_name, sub_tree_prefix, key, value)`.
/// In this impl the `tree_name` can be one of `state`, `log` and `sm`. See [`MetaRaftStore`].
pub fn exported_line_to_json(typ: &str, d: &[Vec<u8>]) -> Result<String, ErrorCode> {
    let prefix_key = &d[0];
    let vec_value = &d[1];

    let prefix = prefix_key[0];
    let vec_key = &prefix_key[1..];

    to_line!(sledks::Logs, typ, prefix, vec_key, vec_value);
    to_line!(sledks::Nodes, typ, prefix, vec_key, vec_value);
    to_line!(sledks::StateMachineMeta, typ, prefix, vec_key, vec_value);
    to_line!(sledks::RaftStateKV, typ, prefix, vec_key, vec_value);
    to_line!(sledks::GenericKV, typ, prefix, vec_key, vec_value);
    to_line!(sledks::Sequences, typ, prefix, vec_key, vec_value);
    to_line!(sledks::Databases, typ, prefix, vec_key, vec_value);
    to_line!(sledks::Tables, typ, prefix, vec_key, vec_value);
    to_line!(sledks::ClientLastResps, typ, prefix, vec_key, vec_value);
    to_line!(sledks::TableLookup, typ, prefix, vec_key, vec_value);
    to_line!(sledks::DatabaseLookup, typ, prefix, vec_key, vec_value);
    to_line!(sledks::LogMeta, typ, prefix, vec_key, vec_value);

    unreachable!("unknown prefix: {}", prefix);
}
