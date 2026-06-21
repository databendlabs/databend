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
use std::time::Instant;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_storages_common_io::ReadSettings;
use log::debug;

use super::block_format::FuseParquetBlockFormat;
use super::parquet_data_source::ParquetDataSource;
use crate::FuseBlockPartInfo;
use crate::FuseStorageFormat;
use crate::io::AggIndexReader;
use crate::io::BlockReadContext;
use crate::io::BlockReadResult;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualBlockReadResult;
use crate::io::VirtualColumnReader;
use crate::operations::read::ReadState;

pub struct ReadBlockContext {
    read_settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_read_ctx: BlockReadContext,
    block_format: FuseParquetBlockFormat,
    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
}

impl ReadBlockContext {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        storage_format: FuseStorageFormat,
        block_read_ctx: BlockReadContext,
        block_format: FuseParquetBlockFormat,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            read_settings: ReadSettings::from_ctx(&ctx)?,
            storage_format,
            block_read_ctx,
            block_format,
            index_reader,
            virtual_reader,
        }))
    }

    #[inline]
    pub fn read_settings(&self) -> ReadSettings {
        self.read_settings
    }

    #[inline]
    pub fn can_split_prewhere(&self) -> bool {
        self.virtual_reader.as_ref().is_none()
    }

    #[async_backtrace::framed]
    pub async fn read_data(&self, part: PartInfoPtr) -> Result<ParquetDataSource> {
        let fuse_part = FuseBlockPartInfo::from_part(&part)?;

        if let Some(data_source) = self.read_agg_index_data(fuse_part).await? {
            return Ok(data_source);
        }

        let virtual_source = self.read_virtual_data(fuse_part).await;
        let ignore_column_ids = virtual_source
            .as_ref()
            .and_then(|source| source.ignore_column_ids.clone());

        let data = self
            .block_format
            .read_data_by_merge_io(
                &self.block_read_ctx,
                &self.read_settings,
                &fuse_part.location,
                &fuse_part.columns_meta,
                &ignore_column_ids,
            )
            .await?;

        Ok(ParquetDataSource::Normal((data, virtual_source)))
    }

    pub async fn read_data_with_prewhere(
        &self,
        part: PartInfoPtr,
        read_state: ReadState,
    ) -> Result<ParquetDataSource> {
        let fuse_part = FuseBlockPartInfo::from_part(&part)?;

        if read_state.use_single_prewhere_reader
            || (read_state.filters.is_none() && read_state.runtime_filters.is_empty())
            || self.virtual_reader.as_ref().is_some()
        {
            return self.read_data(part).await;
        }

        if let Some(data_source) = self.read_agg_index_data(fuse_part).await? {
            return Ok(data_source);
        }

        let pre_data = self
            .read_block_with_reader(&read_state.pre_reader, fuse_part)
            .await?;
        let deserialize_start = Instant::now();
        let pre_columns_chunks = pre_data.columns_chunks()?;
        let prewhere_result = read_state.deserialize_prewhere(pre_columns_chunks, fuse_part)?;

        let no_rows_selected = prewhere_result
            .row_selection
            .as_ref()
            .is_some_and(|row_selection| row_selection.selected_rows == 0);
        let block = if no_rows_selected {
            read_state.deserialize_remaining(
                prewhere_result.preread_block,
                Default::default(),
                fuse_part,
                prewhere_result.row_selection.as_ref(),
                prewhere_result.bitmap_selection.as_ref(),
                prewhere_result.push_down_row_selection,
            )?
        } else {
            let remain_data = self
                .read_block_with_reader(&read_state.remain_reader, fuse_part)
                .await?;
            read_state.deserialize_remaining(
                prewhere_result.preread_block,
                remain_data.columns_chunks()?,
                fuse_part,
                prewhere_result.row_selection.as_ref(),
                prewhere_result.bitmap_selection.as_ref(),
                prewhere_result.push_down_row_selection,
            )?
        };

        Ok(ParquetDataSource::Decoded {
            block,
            bitmap_selection: prewhere_result.bitmap_selection,
            deserialize_milliseconds: deserialize_start.elapsed().as_millis() as u64,
        })
    }

    pub async fn read_block_with_reader(
        &self,
        reader: &BlockReader,
        fuse_part: &FuseBlockPartInfo,
    ) -> Result<BlockReadResult> {
        self.block_format
            .read_data_by_merge_io(
                &reader.read_context(),
                &self.read_settings,
                &fuse_part.location,
                &fuse_part.columns_meta,
                &None,
            )
            .await
    }

    async fn read_agg_index_data(
        &self,
        fuse_part: &FuseBlockPartInfo,
    ) -> Result<Option<ParquetDataSource>> {
        let Some(index_reader) = self.index_reader.as_ref() else {
            return Ok(None);
        };

        let location = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
            &fuse_part.location,
            index_reader.index_id(),
        );
        let index_block_read_ctx = index_reader.block_read_context();

        let Some(block_meta) = self
            .block_format
            .read_block_meta(index_block_read_ctx.operator(), &location)
            .await
        else {
            return Ok(None);
        };

        let data = match self
            .block_format
            .read_data_by_merge_io(
                &index_block_read_ctx,
                &self.read_settings,
                &location,
                &block_meta.columns_meta,
                &None,
            )
            .await
        {
            Ok(data) => data,
            Err(err) => {
                debug!("Read aggregating index `{location}` failed: {err}");
                return Ok(None);
            }
        };

        let part = FuseBlockPartInfo::create(
            location,
            None,
            0,
            None,
            0,
            block_meta.num_rows,
            block_meta.columns_meta,
            None,
            None,
            index_reader.compression().into(),
            None,
            None,
            None,
            None,
        );
        Ok(Some(ParquetDataSource::AggIndex((part, data))))
    }

    async fn read_virtual_data(
        &self,
        fuse_part: &FuseBlockPartInfo,
    ) -> Option<VirtualBlockReadResult> {
        if !matches!(self.storage_format, FuseStorageFormat::Parquet) {
            return None;
        }

        let virtual_reader = self.virtual_reader.as_ref().as_ref()?;
        let virtual_block_meta = fuse_part
            .block_meta_index
            .as_ref()
            .and_then(|block_meta| block_meta.virtual_block_meta.as_ref());

        virtual_reader
            .read_parquet_data_by_merge_io(
                &self.read_settings,
                &virtual_block_meta,
                fuse_part.nums_rows,
            )
            .await
    }
}
