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

use arrow_schema::Schema;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_storages_parquet::InMemoryRowGroup;
use databend_common_storages_parquet::ParquetFileReader;
use databend_common_storages_parquet::ReadSettings as ParquetReadSettings;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_io::ReadSettings;
use log::debug;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::ParquetMetaDataReader;

use super::block_format::FuseParquetBlockFormat;
use super::parquet_data_source::ParquetDataSource;
use crate::FuseBlockPartInfo;
use crate::FuseStorageFormat;
use crate::io::AggIndexReader;
use crate::io::BlockReadContext;
use crate::io::BlockReader;
use crate::io::RowSelection;
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

pub(crate) async fn read_parquet_page_range_data(
    block_reader: &Arc<BlockReader>,
    read_settings: &ReadSettings,
    part: &FuseBlockPartInfo,
) -> Result<Option<DataBlock>> {
    let page_cache_key = block_reader.page_range_data_cache_key(part);
    if let Some(data_block) = page_cache_key
        .as_deref()
        .and_then(|key| block_reader.cached_page_range_data(key))
    {
        return Ok(Some(data_block));
    }

    let block_read_ctx = block_reader.read_context();
    let metadata = parquet_metadata_with_offset_indexes(&block_read_ctx, part).await?;
    if metadata.num_row_groups() != 1 {
        return Ok(None);
    }
    let Some(offset_indexes) = metadata.offset_index().and_then(|v| v.first()) else {
        return Ok(None);
    };

    let schema_descr = metadata.file_metadata().schema_descr();
    let projection_indices = block_read_ctx
        .project_indices()
        .iter()
        .filter_map(|(index, (column_id, ..))| {
            part.columns_meta.contains_key(column_id).then_some(*index)
        })
        .collect::<Vec<_>>();
    if projection_indices.is_empty()
        || projection_indices
            .iter()
            .any(|index| *index >= schema_descr.num_columns())
    {
        return Ok(None);
    }

    let page_bitmap = BlockReader::page_range_bitmap(part)
        .ok_or_else(|| ErrorCode::Internal("page range is missing"))?;
    let parquet_selection = RowSelection::from(&page_bitmap).selection;
    let page_locations = offset_indexes
        .iter()
        .map(|index| index.page_locations().to_vec())
        .collect::<Vec<_>>();
    if page_locations.len() != schema_descr.num_columns() {
        return Ok(None);
    }

    let projection = ProjectionMask::leaves(schema_descr, projection_indices);
    let parquet_read_settings = ParquetReadSettings {
        max_gap_size: read_settings.max_gap_size,
        max_range_size: read_settings.max_range_size,
        parquet_fast_read_bytes: read_settings.parquet_fast_read_bytes,
        enable_cache: true,
    };
    let mut row_group = InMemoryRowGroup::new(
        &part.location,
        block_read_ctx.operator().clone(),
        metadata.row_group(0),
        Some(page_locations),
        parquet_read_settings,
    );
    row_group
        .fetch(&projection, Some(&parquet_selection))
        .await?;

    let arrow_schema = Schema::from(block_reader.original_schema.as_ref());
    let field_levels =
        parquet_to_arrow_field_levels(schema_descr, projection, Some(arrow_schema.fields()))?;
    let mut reader = ParquetRecordBatchReader::try_new_with_row_groups(
        &field_levels,
        &row_group,
        part.nums_rows,
        Some(parquet_selection),
    )?;
    let record_batch = reader
        .next()
        .ok_or_else(|| ErrorCode::Internal("selected parquet range returned no rows"))??;
    debug_assert!(reader.next().is_none());

    let data_block = block_reader.deserialize_parquet_record_batch(part, &record_batch)?;
    Ok(Some(match page_cache_key {
        Some(key) => block_reader.cache_page_range_data(key, data_block),
        None => data_block,
    }))
}

async fn parquet_metadata_with_offset_indexes(
    block_read_ctx: &BlockReadContext,
    part: &FuseBlockPartInfo,
) -> Result<Arc<ParquetMetaData>> {
    let cache = CacheManager::instance().get_parquet_meta_data_cache();
    let cache_key = format!(
        "{}{}",
        block_read_ctx.operator().info().root(),
        part.location
    );
    if let Some(metadata) = cache.as_ref().and_then(|cache| cache.get(&cache_key)) {
        if metadata.offset_index().is_some() {
            return Ok(metadata);
        }
    }

    let op_reader = block_read_ctx.operator().reader(&part.location).await?;
    let mut file_reader = ParquetFileReader::new(op_reader, part.file_size);
    let metadata = ParquetMetaDataReader::new()
        .with_offset_index_policy(PageIndexPolicy::Required)
        .load_and_finish(&mut file_reader, part.file_size)
        .await?;
    Ok(match cache {
        Some(cache) => cache.insert(cache_key, metadata),
        None => Arc::new(metadata),
    })
}
