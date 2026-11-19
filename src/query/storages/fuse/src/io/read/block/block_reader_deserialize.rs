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

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockDataFile;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::column_oriented_segment::BlockReadInfo;

use super::BlockReader;
use crate::BlockReadResult;
use crate::FuseBlockPartInfo;
use crate::FuseStorageFormat;
use crate::io::read::block::block_reader_merge_io::DataItem;
use crate::unsupported_storage_format_error;

impl BlockReader {
    /// Deserialize column chunks data from parquet format to DataBlock.
    pub fn deserialize_chunks_with_part_info(
        &self,
        part: PartInfoPtr,
        chunks: HashMap<ColumnId, DataItem>,
        storage_format: &FuseStorageFormat,
    ) -> Result<DataBlock> {
        let part = FuseBlockPartInfo::from_part(&part)?;
        self.deserialize_files(part.nums_rows, &part.data_files(), chunks, storage_format)
    }

    /// Deserialize the column chunks read from the data files of a block.
    pub fn deserialize_files(
        &self,
        num_rows: usize,
        files: &[BlockDataFile<'_>],
        column_chunks: HashMap<ColumnId, DataItem>,
        storage_format: &FuseStorageFormat,
    ) -> Result<DataBlock> {
        match storage_format {
            FuseStorageFormat::Parquet => {
                self.deserialize_parquet_files(num_rows, files, column_chunks, None)
            }
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
        let files = meta.data_files();
        let data = self.read_files_by_merge_io(settings, &files, &None).await?;
        self.deserialize_files(
            meta.row_count as usize,
            &files,
            data.columns_chunks()?,
            storage_format,
        )
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
