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

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::ColumnMeta;

use crate::operations::read::raw_data_source::RawDataSource;

pub struct ReadBlockMeta {
    pub columns_meta: HashMap<ColumnId, ColumnMeta>,
    pub num_rows: u64,
}

/// Format-specific reader for Fuse blocks.
///
/// The common read transform owns partition dispatch, runtime pruning, and
/// concurrency. Implementations only need to expose basic read primitives for
/// arbitrary block locations and let the common read pipeline decide what the
/// content means.
#[async_trait::async_trait]
pub trait FuseBlockFormat: Send + Sync {
    /// Reads raw column data from the given block location.
    async fn read_data_by_merge_io(
        &self,
        settings: &ReadSettings,
        location: &str,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
        ignore_column_ids: &Option<HashSet<ColumnId>>,
    ) -> Result<RawDataSource>;

    /// Reads the metadata needed to fetch an arbitrary block location.
    async fn read_block_meta(&self, location: &str) -> Option<ReadBlockMeta>;
}
