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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockMetaInfoPtr;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_pipeline_transforms::processors::transforms::transform_accumulating_async::AsyncAccumulatingTransform;
use opendal::Operator;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::Statistics;
use tracing::debug;
use tracing::info;

use crate::io::TableMetaLocationGenerator;
use crate::operations::merge_into::mutation_meta::CommitMeta;
use crate::operations::merge_into::mutation_meta::MutationLogs;
use crate::operations::merge_into::mutator::MutationAccumulator;

// takes in table mutation logs and aggregates them (former mutation_transform)
pub struct TableMutationAggregator {
    ctx: Arc<dyn TableContext>,
    mutation_accumulator: MutationAccumulator,

    start_time: Instant,
    finished_tasks: usize,
}

impl TableMutationAggregator {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        base_segments: Vec<Location>,
        base_summary: Statistics,
        thresholds: BlockThresholds,
        location_gen: TableMetaLocationGenerator,
        schema: TableSchemaRef,
        dal: Operator,
    ) -> Self {
        let mutation_accumulator = MutationAccumulator::new(
            ctx.clone(),
            schema,
            dal,
            location_gen,
            thresholds,
            base_segments,
            base_summary,
        );

        TableMutationAggregator {
            ctx,
            mutation_accumulator,
            start_time: Instant::now(),
            finished_tasks: 0,
        }
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for TableMutationAggregator {
    const NAME: &'static str = "MutationAggregator";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        let mutation = MutationLogs::try_from(data)?;
        for entry in &mutation.entries {
            self.mutation_accumulator.accumulate_log_entry(entry);
        }

        // Refresh status
        {
            self.finished_tasks += mutation.entries.len();
            let status = format!(
                "mutation: run tasks:{}, cost:{} sec",
                self.finished_tasks,
                self.start_time.elapsed().as_secs()
            );
            self.ctx.set_status_info(&status);
            info!(status);
        }
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        let mutations: CommitMeta = self.mutation_accumulator.apply().await?;
        debug!("mutations {:?}", mutations);
        let block_meta: BlockMetaInfoPtr = Box::new(mutations);
        Ok(Some(DataBlock::empty_with_meta(block_meta)))
    }
}
