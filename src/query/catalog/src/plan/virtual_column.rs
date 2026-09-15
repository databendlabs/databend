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

use databend_common_expression::ColumnId;

#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct VirtualColumnPath {
    pub source_column_id: ColumnId,
    pub path: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VirtualColumnLayout {
    pub direct_paths: Vec<VirtualColumnPath>,
}

impl VirtualColumnLayout {
    pub fn contains(&self, source_column_id: ColumnId, path: &str) -> bool {
        self.direct_paths
            .iter()
            .any(|item| item.source_column_id == source_column_id && item.path == path)
    }
}
