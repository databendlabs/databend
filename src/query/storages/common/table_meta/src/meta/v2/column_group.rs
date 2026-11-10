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

use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_frozen_api::FrozenAPI;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::ColumnMeta;
use crate::meta::Compression;
use crate::meta::Location;
use crate::meta::v2::BlockMeta;

/// Bloom index file of one column group file.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct ColumnGroupBloomMeta {
    pub location: Location,
    pub file_size: u64,
}

/// One data file serving a subset of a block's leaf columns.
///
/// Every group file of a block has `BlockMeta::row_count` rows in the same row order, so a
/// reader stitches columns from different group files by row position.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct ColumnGroupFileMeta {
    pub location: Location,
    /// Size of the whole file, not only of the chunks listed in `leaf_column_metas`.
    pub file_size: u64,
    /// Chunk positions of the leaf columns this file serves. Each column id appears in exactly
    /// one group of a block. This is not an inventory of the file: a chunk that a later group
    /// superseded is dropped from this map but stays in the file.
    pub leaf_column_metas: HashMap<ColumnId, ColumnMeta>,
    pub bloom: Option<ColumnGroupBloomMeta>,
    /// Codec of this file. The table compression option can change between the writes of two
    /// groups, so each group records its own.
    pub compression: Compression,
}

impl BlockMeta {
    /// A split block stores its columns across the group files in `column_groups`.
    /// A legacy block stores all columns in the single file at `location`.
    pub fn is_split(&self) -> bool {
        !self.column_groups.is_empty()
    }

    /// Records that `group` now serves every column it stores.
    ///
    /// A legacy block first folds its single data file into group 0 so that no column loses
    /// its chunk. Older groups stop serving the columns `group` stores, and a group left with
    /// no column is dropped.
    pub fn append_column_group(&mut self, group: ColumnGroupFileMeta) -> Result<()> {
        if group.leaf_column_metas.is_empty() {
            return Err(ErrorCode::Internal(format!(
                "column group {} serves no column",
                group.location.0
            )));
        }
        if !self.is_split() {
            if self.col_metas.is_empty() {
                return Err(ErrorCode::Internal(format!(
                    "legacy block {} has no column metas to fold into a column group",
                    self.location.0
                )));
            }
            let legacy = ColumnGroupFileMeta {
                location: self.location.clone(),
                file_size: self.file_size,
                leaf_column_metas: std::mem::take(&mut self.col_metas),
                compression: self.compression,
                bloom: self.bloom_filter_index_location.take().map(|location| {
                    ColumnGroupBloomMeta {
                        location,
                        file_size: self.bloom_filter_index_size,
                    }
                }),
            };
            self.column_groups.push(legacy);
        }
        for older in &mut self.column_groups {
            older
                .leaf_column_metas
                .retain(|id, _| !group.leaf_column_metas.contains_key(id));
        }
        self.column_groups
            .retain(|older| !older.leaf_column_metas.is_empty());
        self.location = group.location.clone();
        self.column_groups.push(group);
        self.file_size = self.column_groups.iter().map(|g| g.file_size).sum();
        self.col_metas = HashMap::new();
        self.bloom_filter_index_location = None;
        self.bloom_filter_index_size = self.column_groups_bloom_size();
        debug_assert!(self.check_column_groups().is_ok());
        Ok(())
    }

    /// Checks the invariants a split block read from storage must satisfy.
    pub fn check_column_groups(&self) -> Result<()> {
        if !self.is_split() {
            return Ok(());
        }
        let block = &self.location.0;
        let fail = |what: String| Err(ErrorCode::Internal(format!("block {block}: {what}")));
        let mut served = HashSet::new();
        for group in &self.column_groups {
            if group.leaf_column_metas.is_empty() {
                return fail(format!(
                    "column group {} serves no column",
                    group.location.0
                ));
            }
            if let Some(id) = group
                .leaf_column_metas
                .keys()
                .find(|id| !served.insert(**id))
            {
                return fail(format!(
                    "column {id} is served by more than one column group"
                ));
            }
        }
        let newest = &self.column_groups[self.column_groups.len() - 1];
        if self.location != newest.location {
            return fail(format!(
                "location differs from the newest column group {}",
                newest.location.0
            ));
        }
        let file_size: u64 = self.column_groups.iter().map(|g| g.file_size).sum();
        if self.file_size != file_size {
            return fail(format!(
                "file_size {} differs from the column group total {file_size}",
                self.file_size
            ));
        }
        if !self.col_metas.is_empty() {
            return fail("col_metas must be empty, column groups own the chunks".to_string());
        }
        if self.bloom_filter_index_location.is_some() {
            return fail("bloom_filter_index_location must be None".to_string());
        }
        let bloom_size = self.column_groups_bloom_size();
        if self.bloom_filter_index_size != bloom_size {
            return fail(format!(
                "bloom_filter_index_size {} differs from the column group total {bloom_size}",
                self.bloom_filter_index_size
            ));
        }
        Ok(())
    }

    fn column_groups_bloom_size(&self) -> u64 {
        self.column_groups
            .iter()
            .filter_map(|g| g.bloom.as_ref())
            .map(|b| b.file_size)
            .sum()
    }
}
