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
use std::future::Future;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::column_oriented_segment::BlockReadInfo;

use super::BlockReader;
use crate::BlockReadResult;
use crate::FuseBlockPartInfo;
use crate::FuseColumnGroupPartInfo;
use crate::FuseStorageFormat;
use crate::fuse_part::project_column_groups;
use crate::io::read::block::block_reader_merge_io::DataItem;
use crate::unsupported_storage_format_error;

impl BlockReader {
    pub(crate) fn projected_column_groups(&self, meta: &BlockMeta) -> Vec<FuseColumnGroupPartInfo> {
        let projected_column_ids = self
            .project_column_nodes
            .iter()
            .flat_map(|node| node.leaf_column_ids.iter().copied())
            .collect::<std::collections::HashSet<_>>();
        project_column_groups(meta, &projected_column_ids)
    }

    /// Deserialize column chunks data from parquet format to DataBlock.
    pub fn deserialize_chunks_with_part_info(
        &self,
        part: PartInfoPtr,
        chunks: HashMap<ColumnId, DataItem>,
        storage_format: &FuseStorageFormat,
    ) -> Result<DataBlock> {
        let part = FuseBlockPartInfo::from_part(&part)?;
        match storage_format {
            FuseStorageFormat::Parquet => self.deserialize_part(part, chunks, None),
            FuseStorageFormat::Unsupported => Err(unsupported_storage_format_error()),
        }
    }

    pub fn deserialize_chunks(
        &self,
        block_path: &str,
        num_rows: usize,
        compression: &Compression,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        storage_format: &FuseStorageFormat,
    ) -> Result<DataBlock> {
        match storage_format {
            FuseStorageFormat::Parquet => self.deserialize_parquet_chunks(
                num_rows,
                column_metas,
                column_chunks,
                compression,
                block_path,
                None,
            ),
            FuseStorageFormat::Unsupported => Err(unsupported_storage_format_error()),
        }
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn read_by_meta(
        &self,
        settings: &ReadSettings,
        meta: &BlockMeta,
        storage_format: &FuseStorageFormat,
    ) -> Result<DataBlock> {
        // Get the merged IO read result.
        let column_groups = self.projected_column_groups(meta);
        // Type erasure breaks the recursive async future formed by virtual-column reads that
        // return to `read_by_meta` through the merge-IO path.
        let read: std::pin::Pin<Box<dyn Future<Output = Result<BlockReadResult>> + Send + '_>> =
            Box::pin(self.read_column_groups_data_by_merge_io(settings, &column_groups, &None));
        let merge_io_read_result = read.await?;
        let column_chunks = merge_io_read_result.columns_chunks()?;
        match storage_format {
            FuseStorageFormat::Parquet => self.deserialize_column_groups(
                meta.row_count as usize,
                &column_groups,
                column_chunks,
                &meta.compression,
                None,
            ),
            FuseStorageFormat::Unsupported => Err(unsupported_storage_format_error()),
        }
    }

    pub fn deserialize_chunks_with_meta(
        &self,
        meta: &BlockReadInfo,
        storage_format: &FuseStorageFormat,
        data: BlockReadResult,
    ) -> Result<DataBlock> {
        // Get the columns chunk.
        let column_chunks = data.columns_chunks()?;

        let num_rows = meta.row_count as usize;

        match storage_format {
            FuseStorageFormat::Parquet => self.deserialize_parquet_chunks(
                num_rows,
                &meta.col_metas,
                column_chunks,
                &meta.compression,
                &meta.location,
                None,
            ),
            FuseStorageFormat::Unsupported => Err(unsupported_storage_format_error()),
        }
    }
}
