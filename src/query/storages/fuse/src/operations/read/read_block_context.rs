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

use super::block_format::FuseBlockFormat;
use super::native_data_source::NativeDataSource;
use super::parquet_data_source::ParquetDataSource;
use super::raw_data_source::RawDataSource;
use super::read_data_source::ReadDataSource;
use crate::FuseBlockPartInfo;
use crate::FuseStorageFormat;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualBlockReadResult;
use crate::io::VirtualColumnReader;

pub struct ReadBlockContext {
    read_settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_reader: Arc<BlockReader>,
    block_format: Arc<dyn FuseBlockFormat>,
    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
}

impl ReadBlockContext {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        storage_format: FuseStorageFormat,
        block_reader: Arc<BlockReader>,
        block_format: Arc<dyn FuseBlockFormat>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            read_settings: ReadSettings::from_ctx(&ctx)?,
            storage_format,
            block_reader,
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
    pub async fn read_data(&self, part: PartInfoPtr) -> Result<ReadDataSource> {
        let fuse_part = FuseBlockPartInfo::from_part(&part)?;

        if let Some(data_source) = self.read_agg_index_data(fuse_part).await? {
            return Ok(data_source);
        }

        let virtual_source = self.read_virtual_data(fuse_part).await;
        let ignore_column_ids = virtual_source
            .as_ref()
            .and_then(|source| source.ignore_column_ids.clone());

        let raw_data = self
            .block_format
            .read_data_by_merge_io(
                &self.block_reader,
                &self.read_settings,
                &fuse_part.location,
                &fuse_part.columns_meta,
                &ignore_column_ids,
            )
            .await?;

        Ok(match raw_data {
            RawDataSource::Native(data) => {
                ReadDataSource::Native(Box::new(NativeDataSource::Normal(data)))
            }
            RawDataSource::Parquet(data) => {
                ReadDataSource::Parquet(Box::new(ParquetDataSource::Normal((data, virtual_source))))
            }
        })
    }

    async fn read_agg_index_data(
        &self,
        fuse_part: &FuseBlockPartInfo,
    ) -> Result<Option<ReadDataSource>> {
        let Some(index_reader) = self.index_reader.as_ref() else {
            return Ok(None);
        };

        let location = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
            &fuse_part.location,
            index_reader.index_id(),
        );
        let index_block_reader = index_reader.block_reader();

        let Some(block_meta) = self
            .block_format
            .read_block_meta(index_block_reader.as_ref(), &location)
            .await
        else {
            return Ok(None);
        };

        let raw_data = match self
            .block_format
            .read_data_by_merge_io(
                index_block_reader.as_ref(),
                &self.read_settings,
                &location,
                &block_meta.columns_meta,
                &None,
            )
            .await
        {
            Ok(raw_data) => raw_data,
            Err(err) => {
                debug!("Read aggregating index `{location}` failed: {err}");
                return Ok(None);
            }
        };

        Ok(Some(match raw_data {
            RawDataSource::Native(data) => {
                ReadDataSource::Native(Box::new(NativeDataSource::AggIndex(data)))
            }
            RawDataSource::Parquet(data) => {
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
                );
                ReadDataSource::Parquet(Box::new(ParquetDataSource::AggIndex((part, data))))
            }
        }))
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
