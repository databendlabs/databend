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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_io::ReadSettings;
use log::debug;

use super::block_format::FuseParquetBlockFormat;
use super::granule_group::GranuleGroupsReadPlan;
use super::granule_group::build_granule_groups;
use super::parquet_data_source::ParquetDataSource;
use crate::FuseBlockPartInfo;
use crate::FuseStorageFormat;
use crate::io::AggIndexReader;
use crate::io::BlockReadContext;
use crate::io::GranuleDataReader;
use crate::io::OffsetsIndex;
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
    max_block_size: usize,
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
            max_block_size: ctx.get_settings().get_max_block_size()? as usize,
        }))
    }

    #[inline]
    pub fn read_settings(&self) -> ReadSettings {
        self.read_settings
    }

    #[async_backtrace::framed]
    pub(crate) async fn read_full_data(&self, part: PartInfoPtr) -> Result<ParquetDataSource> {
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

        Ok(ParquetDataSource::Normal((vec![data], virtual_source)))
    }

    pub(crate) fn build_granule_groups_if_subset(
        &self,
        part: &PartInfoPtr,
    ) -> Result<Option<GranuleGroupsReadPlan>> {
        if self.index_reader.is_some() || self.virtual_reader.is_some() {
            return Ok(None);
        }
        let fuse_part = FuseBlockPartInfo::from_part(part)?;
        let Some(granule_index) = fuse_part.granule_index.as_ref() else {
            return Ok(None);
        };
        let Some(ranges) = fuse_part
            .block_meta_index()
            .and_then(|index| index.granule_ranges.as_ref())
        else {
            return Ok(None);
        };
        if ranges.is_empty() {
            return Err(ErrorCode::Internal(
                "granule-pruned part contains no ranges",
            ));
        }

        let num_granules =
            crate::io::num_granules_of(fuse_part.nums_rows, granule_index.granule_rows as usize);
        let mut selected = 0usize;
        let mut previous_end = None;
        for range in ranges {
            if range.start >= range.end || range.end > num_granules {
                return Err(ErrorCode::Internal(format!(
                    "invalid granule range {range:?} for {num_granules} granules"
                )));
            }
            if previous_end.is_some_and(|end| range.start < end) {
                return Err(ErrorCode::Internal(format!(
                    "overlapping or unordered granule ranges near {range:?}"
                )));
            }
            selected = selected
                .checked_add(range.end - range.start)
                .ok_or_else(|| ErrorCode::Internal("selected granule count overflows"))?;
            previous_end = Some(range.end);
        }
        if selected >= num_granules {
            return Ok(None);
        }

        Ok(Some(GranuleGroupsReadPlan {
            groups: build_granule_groups(
                ranges,
                granule_index.granule_rows as usize,
                fuse_part.nums_rows,
                self.max_block_size,
            )?,
        }))
    }

    pub(crate) fn create_granule_data_reader(
        &self,
        part: &PartInfoPtr,
        plan: &GranuleGroupsReadPlan,
    ) -> Result<GranuleDataReader> {
        let fuse_part = FuseBlockPartInfo::from_part(part)?;
        let granule_index = fuse_part
            .granule_index
            .as_ref()
            .ok_or_else(|| ErrorCode::Internal("granule index metadata is missing"))?;
        let offsets = OffsetsIndex::load(
            self.block_read_ctx.operator(),
            &self.read_settings,
            &granule_index.offsets,
            granule_index.granule_rows as usize,
            fuse_part.nums_rows,
            &fuse_part.columns_meta,
        )?;
        GranuleDataReader::create(
            &self.block_read_ctx,
            &self.read_settings,
            fuse_part,
            plan,
            &offsets,
        )
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
            block_meta.num_rows,
            block_meta.columns_meta,
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
