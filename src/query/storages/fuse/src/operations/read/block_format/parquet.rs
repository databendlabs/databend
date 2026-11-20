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

use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockDataFile;

use crate::io::BlockReadContext;
use crate::io::BlockReadResult;

pub struct FuseParquetBlockFormat;

impl FuseParquetBlockFormat {
    pub fn create() -> Self {
        Self
    }

    /// Reads raw column data from the data files of a block.
    pub async fn read_data_by_merge_io(
        &self,
        read_ctx: &BlockReadContext,
        settings: &ReadSettings,
        files: &[BlockDataFile<'_>],
        ignore_column_ids: &Option<HashSet<ColumnId>>,
    ) -> Result<BlockReadResult> {
        read_ctx
            .read_files_by_merge_io(settings, files, ignore_column_ids)
            .await
    }
}
