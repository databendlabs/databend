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
use std::sync::Arc;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_cache::CacheLockStats;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::SingleColumnMeta;

use super::block_format::FuseParquetBlockFormat;
use super::granule_group::build_granule_groups;
use super::parquet_data_source::ParquetDataSource;
use crate::FuseBlockPartInfo;
use crate::FuseStorageFormat;
use crate::io::BlockReadContext;
use crate::io::GranuleDataReader;
use crate::io::OffsetsIndex;
use crate::io::VirtualBlockReadResult;
use crate::io::VirtualColumnReader;

pub struct ReadBlockContext {
    read_settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_read_ctx: BlockReadContext,
    block_format: FuseParquetBlockFormat,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
    max_block_size: usize,
}

impl ReadBlockContext {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        storage_format: FuseStorageFormat,
        block_read_ctx: BlockReadContext,
        block_format: FuseParquetBlockFormat,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            read_settings: ReadSettings::from_ctx(&ctx)?,
            storage_format,
            block_read_ctx,
            block_format,
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

    pub(crate) fn granule_groups_if_subset(
        &self,
        part: &PartInfoPtr,
    ) -> Result<Option<Vec<Vec<std::ops::Range<usize>>>>> {
        let fuse_part = FuseBlockPartInfo::from_part(part)?;
        let Some(ranges) = fuse_part
            .block_meta_index()
            .and_then(|index| index.granule_ranges.as_deref())
        else {
            return Ok(None);
        };
        let Some(groups) = self.granule_groups(part, Some(ranges))? else {
            return Ok(None);
        };
        let granule_rows = fuse_part
            .granule_index
            .as_ref()
            .expect("granule_groups checked metadata")
            .granule_rows as usize;
        let num_granules = crate::io::num_granules_of(fuse_part.nums_rows, granule_rows);
        let selected = ranges.iter().try_fold(0usize, |selected, range| {
            selected
                .checked_add(range.end - range.start)
                .ok_or_else(|| ErrorCode::Internal("selected granule count overflows"))
        })?;
        Ok((selected < num_granules).then_some(groups))
    }

    pub(crate) fn granule_groups(
        &self,
        part: &PartInfoPtr,
        ranges: Option<&[std::ops::Range<usize>]>,
    ) -> Result<Option<Vec<Vec<std::ops::Range<usize>>>>> {
        let fuse_part = FuseBlockPartInfo::from_part(part)?;
        let Some(granule_index) = fuse_part.granule_index.as_ref() else {
            return Ok(None);
        };
        let virtual_meta = fuse_part
            .block_meta_index()
            .and_then(|index| index.virtual_block_meta.as_ref());
        if let (Some(_), Some(meta)) = (self.virtual_reader.as_ref(), virtual_meta) {
            // Old files and refreshed virtual columns may have no matching marks.
            // The presence of virtual columns alone must not disable granule reads.
            for column in meta.virtual_column_metas.values() {
                let mark =
                    crate::io::virtual_offset_mark(&meta.virtual_block_location, column.offset);
                if !granule_index.offsets.columns.contains_key(&mark) {
                    return Ok(None);
                }
            }
        }
        if fuse_part.nums_rows == 0 {
            return Ok(None);
        }
        Ok(Some(build_granule_groups(
            ranges,
            granule_index.granule_rows as usize,
            fuse_part.nums_rows,
            self.max_block_size,
        )?))
    }

    pub(crate) fn create_granule_data_reader(
        &self,
        part: &PartInfoPtr,
        groups: &[Vec<std::ops::Range<usize>>],
        lock_stats: Arc<CacheLockStats>,
    ) -> Result<GranuleDataReader> {
        let fuse_part = FuseBlockPartInfo::from_part(part)?;
        let granule_index = fuse_part
            .granule_index
            .as_ref()
            .ok_or_else(|| ErrorCode::Internal("granule index metadata is missing"))?;
        let ignore_column_ids = self.virtual_reader.as_ref().as_ref().and_then(|reader| {
            fuse_part
                .block_meta_index()
                .and_then(|index| index.virtual_block_meta.as_ref())
                .and_then(|meta| reader.generate_ignore_column_ids(&meta.ignored_source_column_ids))
        });
        let offsets = OffsetsIndex::load_with_stats(
            self.block_read_ctx.operator(),
            &self.read_settings,
            &granule_index.offsets,
            granule_index.granule_rows as usize,
            fuse_part.nums_rows,
            &fuse_part.columns_meta,
            self.block_read_ctx
                .project_indices()
                .values()
                .map(|(column_id, ..)| *column_id)
                .filter(|id| {
                    !ignore_column_ids
                        .as_ref()
                        .is_some_and(|ignored| ignored.contains(id))
                }),
            Some(lock_stats.clone()),
        )?;
        GranuleDataReader::create(
            &self.block_read_ctx,
            &self.read_settings,
            fuse_part,
            groups,
            &offsets,
            ignore_column_ids.as_ref(),
            Some(lock_stats),
        )
    }

    pub(crate) fn create_virtual_granule_reader(
        &self,
        part: &PartInfoPtr,
        groups: &[Vec<std::ops::Range<usize>>],
        lock_stats: Arc<CacheLockStats>,
    ) -> Result<Option<GranuleDataReader>> {
        if self.virtual_reader.is_none() {
            return Ok(None);
        }
        let part = FuseBlockPartInfo::from_part(part)?;
        let Some(meta) = part
            .block_meta_index()
            .and_then(|index| index.virtual_block_meta.as_ref())
        else {
            return Ok(None);
        };
        let layout = part
            .granule_index
            .as_ref()
            .ok_or_else(|| ErrorCode::Internal("missing granule index"))?;
        let columns = meta
            .virtual_column_metas
            .iter()
            .map(|(id, column)| {
                (
                    *id,
                    ColumnMeta::Parquet(SingleColumnMeta {
                        offset: column.offset,
                        len: column.len,
                        num_values: column.num_values,
                    }),
                )
            })
            .collect::<HashMap<_, _>>();
        let offsets = OffsetsIndex::load_named_with_stats(
            self.block_read_ctx.operator(),
            &self.read_settings,
            &layout.offsets,
            layout.granule_rows as usize,
            part.nums_rows,
            &columns,
            meta.virtual_column_metas.iter().map(|(id, column)| {
                (
                    *id,
                    crate::io::virtual_offset_mark(&meta.virtual_block_location, column.offset),
                )
            }),
            Some(lock_stats.clone()),
        )?;
        // The largest projected chunk end is sufficient: readers never request footer bytes.
        let file_len = columns
            .values()
            .map(|meta| {
                let (offset, len) = meta.offset_length();
                offset + len
            })
            .max()
            .unwrap_or(0);
        let schema = VirtualColumnReader::read_schema(meta);
        let column_types = schema.leaf_fields().into_iter().map(|field| {
            let data_type = databend_common_expression::types::DataType::from(field.data_type());
            (field.column_id, data_type.to_string())
        });
        Ok(Some(GranuleDataReader::create_for_file(
            &self.block_read_ctx,
            &self.read_settings,
            &meta.virtual_block_location,
            file_len,
            part.nums_rows,
            &columns,
            column_types,
            groups,
            &offsets,
            Some(lock_stats),
        )?))
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
