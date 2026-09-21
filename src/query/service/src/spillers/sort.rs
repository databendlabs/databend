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

use databend_base::uniq_id::GlobalUniq;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::traits::SortSpiller;
use databend_common_pipeline_transforms::traits::SpillReader;
use databend_common_storage::DataOperator;
use opendal::Buffer;
use opendal::Operator;

use super::Location;
use super::SpillTarget;
use super::SpillsBufferPool;
use super::record_read_profile;
use super::record_write_profile;
use super::serialize::Layout;
use super::serialize::deserialize_block;
use super::serialize::serialize_blocks;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

#[derive(Clone)]
pub struct SortSpillerImpl(Arc<SortStorage>);

struct SortStorage {
    ctx: Arc<QueryContext>,
    operator: Operator,
    buffer_pool: Arc<SpillsBufferPool>,
    memory_settings: MemorySettings,
    use_parquet: bool,
    writer_pool_bytes: usize,
    target: SpillTarget,
}

impl SortSpillerImpl {
    pub fn new(ctx: Arc<QueryContext>) -> Result<Self> {
        let settings = ctx.get_settings();
        let data_operator = DataOperator::instance();
        Ok(Self(Arc::new(SortStorage {
            memory_settings: MemorySettings::from_sort_settings(&ctx)?,
            use_parquet: settings.get_spilling_file_format()?.is_parquet(),
            writer_pool_bytes: settings
                .get_spill_writer_memory_pool_size_mb()?
                .saturating_mul(1024 * 1024),
            operator: data_operator.spill_operator(),
            target: SpillTarget::from_storage_params(data_operator.spill_params()),
            buffer_pool: SpillsBufferPool::instance(),
            ctx,
        })))
    }
}

impl SortSpiller for SortSpillerImpl {
    type Reader = SortReader;

    fn spill(&self, block: DataBlock) -> Result<String> {
        let start = Instant::now();
        let storage = &self.0;
        let path = format!(
            "{}/{}",
            storage.ctx.query_id_spill_prefix(),
            GlobalUniq::unique()
        );
        let mut writer = storage.buffer_pool.buffer_writer(
            storage.operator.clone(),
            path.clone(),
            storage.writer_pool_bytes,
        )?;
        let (layout, size) = serialize_blocks(vec![block], storage.use_parquet, &mut writer)?;
        writer.close()?;
        // Publish only after the background writer has closed the complete file.
        storage
            .ctx
            .add_spill_file(Location::Remote(path.clone()), layout);
        storage.ctx.incr_spill_progress(1, size);
        record_write_profile(storage.target, start.elapsed(), size);
        Ok(path)
    }

    fn reader(&self, path: &str) -> Result<SortReader> {
        let storage = &self.0;
        let layout = storage
            .ctx
            .get_spill_layout(&Location::Remote(path.to_owned()))
            .ok_or_else(|| ErrorCode::Internal(format!("Missing sort spill layout: {path}")))?;
        Ok(SortReader {
            receiver: storage
                .buffer_pool
                .read_buffer(storage.operator.clone(), path.to_owned()),
            layout,
            target: storage.target,
        })
    }

    fn memory_settings(&self) -> &MemorySettings {
        &self.0.memory_settings
    }
}

pub struct SortReader {
    receiver: async_channel::Receiver<Result<Buffer>>,
    layout: Layout,
    target: SpillTarget,
}

impl SpillReader for SortReader {
    fn read(self) -> Result<DataBlock> {
        let start = Instant::now();
        let data = self
            .receiver
            .recv_blocking()
            .map_err(|err| ErrorCode::Internal(format!("Sort spill read interrupted: {err}")))??;
        let size = data.len();
        let block = deserialize_block(&self.layout, data)?;
        record_read_profile(self.target, start.elapsed(), size);
        Ok(block)
    }
}
