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

use super::FuseBlockFormat;
use crate::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::operations::read::native_data_source::NativeDataSource;
use crate::operations::read::read_data_source::ReadDataSource;

pub struct FuseNativeBlockFormat {
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,
    index_reader: Arc<Option<AggIndexReader>>,
}

impl FuseNativeBlockFormat {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
    ) -> Arc<dyn FuseBlockFormat> {
        Arc::new(Self {
            ctx,
            block_reader,
            index_reader,
        })
    }
}

#[async_trait::async_trait]
impl FuseBlockFormat for FuseNativeBlockFormat {
    #[async_backtrace::framed]
    async fn read_data(
        &self,
        part: PartInfoPtr,
        _settings: ReadSettings,
    ) -> Result<ReadDataSource> {
        let fuse_part = FuseBlockPartInfo::from_part(&part)?;

        if let Some(index_reader) = self.index_reader.as_ref() {
            let loc = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                &fuse_part.location,
                index_reader.index_id(),
            );
            if let Some(data) = index_reader.read_native_data(&loc).await {
                return Ok(ReadDataSource::Native(Box::new(
                    NativeDataSource::AggIndex(data),
                )));
            }
        }

        Ok(ReadDataSource::Native(Box::new(NativeDataSource::Normal(
            self.block_reader
                .async_read_native_columns_data(&part, &self.ctx, &None)
                .await?,
        ))))
    }
}
