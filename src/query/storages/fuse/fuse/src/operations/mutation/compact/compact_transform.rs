// Copyright 2022 Datafuse Labs.
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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use common_arrow::parquet::compression::CompressionOptions;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_cache::Cache;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::ChunkCompactThresholds;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::StatisticsOfColumns;
use opendal::Operator;

use super::compact_meta::CompactSourceMeta;
use super::compact_part::CompactTask;
use super::CompactSinkMeta;
use crate::io::write_data;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::AbortOperation;
use crate::operations::util;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::statistics::reduce_block_statistics;
use crate::statistics::reducers::reduce_block_metas;

struct SerializeState {
    block_data: Vec<u8>,
    block_location: String,
    index_data: Vec<u8>,
    index_location: String,
}

enum State {
    Consume,
    Compact(Chunk),
    Generate(Vec<Arc<BlockMeta>>),
    Serialized {
        data: Vec<u8>,
        location: String,
        segment: Arc<SegmentInfo>,
    },
    Output {
        location: String,
        segment: Arc<SegmentInfo>,
    },
}

// Gets a set of CompactTask, only merge but not split, generate a new segment.
pub struct CompactTransform {
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,
    output_data: Option<Chunk>,

    block_reader: Arc<BlockReader>,
    location_gen: TableMetaLocationGenerator,
    dal: Operator,

    // Limit the memory size of the block read.
    max_memory: u64,
    max_io_requests: usize,
    compact_tasks: VecDeque<CompactTask>,
    block_metas: Vec<Arc<BlockMeta>>,
    order: usize,
    thresholds: ChunkCompactThresholds,
    abort_operation: AbortOperation,
}

impl CompactTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        scan_progress: Arc<Progress>,
        block_reader: Arc<BlockReader>,
        location_gen: TableMetaLocationGenerator,
        dal: Operator,
        thresholds: ChunkCompactThresholds,
    ) -> Result<ProcessorPtr> {
        let settings = ctx.get_settings();
        let max_memory_usage = (settings.get_max_memory_usage()? as f64 * 0.8) as u64;
        let max_threads = settings.get_max_threads()?;
        let max_memory = max_memory_usage / max_threads;
        let max_io_requests = settings.get_max_storage_io_requests()? as usize;
        Ok(ProcessorPtr::create(Box::new(CompactTransform {
            state: State::Consume,
            input,
            output,
            scan_progress,
            output_data: None,
            block_reader,
            location_gen,
            dal,
            max_memory,
            max_io_requests,
            compact_tasks: VecDeque::new(),
            block_metas: Vec::new(),
            order: 0,
            thresholds,
            abort_operation: AbortOperation::default(),
        })))
    }
}

#[async_trait::async_trait]
impl Processor for CompactTransform {
    fn name(&self) -> String {
        "CompactTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        todo!("expression");
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            _ => todo!("expression"),
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            _ => todo!("expression"),
        }
        Ok(())
    }
}
