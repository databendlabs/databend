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
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualBlockReadResult;
use crate::io::VirtualColumnReader;

pub struct ReadBlockContext {
    read_settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_read_ctx: BlockReadContext,
    block_format: FuseParquetBlockFormat,
    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
    /// Whether sparse-page-index byte-range narrowing may be applied. Disabled when the query
    /// reads internal columns, virtual columns, or stream columns, or builds merge-into block
    /// indexes — all of which depend on block-relative row positions that narrowing would shift.
    allow_page_index_skip: bool,
}

impl ReadBlockContext {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        storage_format: FuseStorageFormat,
        block_read_ctx: BlockReadContext,
        block_format: FuseParquetBlockFormat,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
        allow_page_index_skip: bool,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            read_settings: ReadSettings::from_ctx(&ctx)?,
            storage_format,
            block_read_ctx,
            block_format,
            index_reader,
            virtual_reader,
            allow_page_index_skip,
        }))
    }

    #[inline]
    pub fn read_settings(&self) -> ReadSettings {
        self.read_settings
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

        // Sparse-page-index narrowed read: when prune time selected a granule run and the query is
        // eligible (no position-dependent columns) and no virtual columns are projected, fetch only
        // the matching granules' byte ranges instead of the whole block.
        if self.allow_page_index_skip && ignore_column_ids.is_none() {
            if let Some(data) = self.try_read_data_by_page_index(fuse_part).await? {
                return Ok(ParquetDataSource::Normal((data, virtual_source)));
            }
        }

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

    /// Attempt a sparse-page-index narrowed read for `fuse_part`. Returns `None` when narrowing
    /// does not apply (no granule range carried, or no sidecar index), so the caller falls back to
    /// a full-block read. A failure to load/decode the sidecar degrades to `None` as well.
    async fn try_read_data_by_page_index(
        &self,
        fuse_part: &FuseBlockPartInfo,
    ) -> Result<Option<crate::io::BlockReadResult>> {
        let Some(block_meta_index) = fuse_part.block_meta_index() else {
            return Ok(None);
        };
        let Some(granule_range) = block_meta_index.page_granule_range.clone() else {
            return Ok(None);
        };
        let Some(location) = fuse_part.page_index_location.as_ref() else {
            return Ok(None);
        };

        let index = match crate::io::PageIndex::load(
            self.block_read_ctx.operator(),
            &location.0,
            fuse_part.page_index_size,
        )
        .await
        {
            Ok(index) => index,
            Err(e) => {
                debug!(
                    "[FUSE-READ] page index load failed for {}, reading whole block: {e}",
                    fuse_part.location
                );
                return Ok(None);
            }
        };

        let plan = index.read_plan_for_range(&granule_range, fuse_part.nums_rows);
        let data = self
            .block_read_ctx
            .read_columns_data_by_page_index(&self.read_settings, &fuse_part.location, &plan)
            .await?;
        Ok(Some(data))
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
