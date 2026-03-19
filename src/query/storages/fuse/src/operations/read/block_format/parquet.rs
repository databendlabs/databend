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

use std::sync::Arc;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::Result;
use databend_storages_common_io::ReadSettings;

use super::FuseBlockFormat;
use crate::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::parquet_data_source::ParquetDataSource;
use crate::operations::read::read_data_source::ReadDataSource;

pub struct FuseParquetBlockFormat {
    block_reader: Arc<BlockReader>,
    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
}

impl FuseParquetBlockFormat {
    pub fn create(
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Arc<dyn FuseBlockFormat> {
        Arc::new(Self {
            block_reader,
            index_reader,
            virtual_reader,
        })
    }
}

#[async_trait::async_trait]
impl FuseBlockFormat for FuseParquetBlockFormat {
    #[async_backtrace::framed]
    async fn read_data(&self, part: PartInfoPtr, settings: ReadSettings) -> Result<ReadDataSource> {
        let fuse_part = FuseBlockPartInfo::from_part(&part)?;

        if let Some(index_reader) = self.index_reader.as_ref() {
            let loc = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                &fuse_part.location,
                index_reader.index_id(),
            );
            if let Some(data) = index_reader
                .read_parquet_data_by_merge_io(&settings, &loc)
                .await
            {
                return Ok(ReadDataSource::Parquet(Box::new(
                    ParquetDataSource::AggIndex(data),
                )));
            }
        }

        // If virtual column file exists, read the data from the virtual columns directly.
        let virtual_source = if let Some(virtual_reader) = self.virtual_reader.as_ref() {
            let virtual_block_meta = fuse_part
                .block_meta_index
                .as_ref()
                .and_then(|b| b.virtual_block_meta.as_ref());
            virtual_reader
                .read_parquet_data_by_merge_io(&settings, &virtual_block_meta, fuse_part.nums_rows)
                .await
        } else {
            None
        };

        let ignore_column_ids = if let Some(virtual_source) = &virtual_source {
            &virtual_source.ignore_column_ids
        } else {
            &None
        };

        let source = self
            .block_reader
            .read_columns_data_by_merge_io(
                &settings,
                &fuse_part.location,
                &fuse_part.columns_meta,
                ignore_column_ids,
            )
            .await?;

        Ok(ReadDataSource::Parquet(Box::new(
            ParquetDataSource::Normal((source, virtual_source)),
        )))
    }
}
