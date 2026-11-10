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

use std::collections::BTreeMap;
use std::num::ParseIntError;

use databend_meta_client::kvapi;

pub const OPT_KEY_CLONE_GROUP_ID: &str = "clone_group_id";

pub fn parse_clone_group_id(
    options: &BTreeMap<String, String>,
) -> Result<Option<u64>, ParseIntError> {
    options
        .get(OPT_KEY_CLONE_GROUP_ID)
        .map(|value| value.parse())
        .transpose()
}

/// A clone table indexed by its stable clone group.
///
/// `__fd_table_clone_by_group/<clone_group_id>/<clone_table_id>`
///     `-> TableCloneBinding`
#[derive(Clone, Debug, Copy, Default, Eq, PartialEq, kvapi::StructKey)]
#[structkey(prefix = "__fd_table_clone_by_group")]
pub struct TableCloneByGroupIdent {
    pub clone_group_id: u64,
    pub clone_table_id: u64,
}

impl TableCloneByGroupIdent {
    pub fn new(clone_group_id: u64, clone_table_id: u64) -> Self {
        Self {
            clone_group_id,
            clone_table_id,
        }
    }
}

/// The direct source used to create a clone table.
#[derive(Clone, Debug, Copy, Default, Eq, PartialEq)]
pub struct TableCloneBinding {
    pub source_table_id: u64,
}

impl kvapi::Key for TableCloneByGroupIdent {
    type ValueType = TableCloneBinding;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::OPT_KEY_CLONE_GROUP_ID;
    use super::TableCloneByGroupIdent;
    use super::parse_clone_group_id;

    #[test]
    fn test_table_clone_by_group_ident() {
        assert_round_trip(
            TableCloneByGroupIdent::new(42, 7),
            "__fd_table_clone_by_group/42/7",
        );
    }

    #[test]
    fn test_parse_clone_group_id() {
        let mut options = BTreeMap::new();
        assert_eq!(parse_clone_group_id(&options).unwrap(), None);
        options.insert(OPT_KEY_CLONE_GROUP_ID.to_string(), "42".to_string());
        assert_eq!(parse_clone_group_id(&options).unwrap(), Some(42));
        options.insert(OPT_KEY_CLONE_GROUP_ID.to_string(), "invalid".to_string());
        assert!(parse_clone_group_id(&options).is_err());
    }
}
