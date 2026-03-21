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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_storage::read_metadata_async;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::ColumnMeta;
use opendal::Operator;

use super::FuseBlockFormat;
use super::ReadBlockMeta;
use crate::io::BlockReadContext;
use crate::io::build_columns_meta;
use crate::operations::read::raw_data_source::RawDataSource;

pub struct FuseParquetBlockFormat;

impl FuseParquetBlockFormat {
    pub fn create() -> Arc<dyn FuseBlockFormat> {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl FuseBlockFormat for FuseParquetBlockFormat {
    #[async_backtrace::framed]
    async fn read_data_by_merge_io(
        &self,
        read_ctx: &BlockReadContext,
        settings: &ReadSettings,
        location: &str,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
        ignore_column_ids: &Option<HashSet<ColumnId>>,
    ) -> Result<RawDataSource> {
        let source = read_ctx
            .read_columns_data_by_merge_io(settings, location, columns_meta, ignore_column_ids)
            .await?;

        Ok(RawDataSource::Parquet(source))
    }

    async fn read_block_meta(&self, operator: &Operator, location: &str) -> Option<ReadBlockMeta> {
        let metadata = read_metadata_async(location, operator, None).await.ok()?;
        debug_assert_eq!(metadata.num_row_groups(), 1);
        let row_group = &metadata.row_groups()[0];

        Some(ReadBlockMeta {
            columns_meta: build_columns_meta(row_group),
            num_rows: row_group.num_rows() as u64,
        })
    }
}
