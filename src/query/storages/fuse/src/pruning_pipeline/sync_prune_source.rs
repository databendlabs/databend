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

use async_channel::Sender;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReadPartitionsPruningMode;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;

use crate::FuseTable;
use crate::SegmentLocation;

/// `enable_prune_pipeline = 0`: prune all lazy segments in one
/// `prune_snapshot_blocks` call instead of a streaming prune pipeline.
///
/// It is still a (one-processor) pipeline so that every caller that builds a
/// prune pipeline — `TableScan`, `EXPLAIN`, `EXPLAIN ANALYZE` — executes it
/// the same way. The source produces no blocks; it forwards the pruned parts
/// over `part_info_tx` and publishes what was scanned under `plan_id`
/// (`partitions_total` was published once by the planner).
pub struct SyncPruneSource {
    state: Option<SyncPruneState>,
}

struct SyncPruneState {
    ctx: Arc<dyn TableContext>,
    table: FuseTable,
    table_schema: TableSchemaRef,
    push_downs: Option<PushDownInfo>,
    lazy_init_segments: Vec<SegmentLocation>,
    part_info_tx: Sender<Result<PartInfoPtr>>,
    plan_id: u32,
}

impl SyncPruneSource {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        table: FuseTable,
        table_schema: TableSchemaRef,
        push_downs: Option<PushDownInfo>,
        lazy_init_segments: Vec<SegmentLocation>,
        part_info_tx: Sender<Result<PartInfoPtr>>,
        plan_id: u32,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, Self {
            state: Some(SyncPruneState {
                ctx,
                table,
                table_schema,
                push_downs,
                lazy_init_segments,
                part_info_tx,
                plan_id,
            }),
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for SyncPruneSource {
    const NAME: &'static str = "SyncPruneSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let Some(state) = self.state.take() else {
            return Ok(None);
        };
        match state
            .table
            .prune_snapshot_blocks(
                state.ctx.clone(),
                state.push_downs,
                state.table_schema,
                state.lazy_init_segments,
                0,
                ReadPartitionsPruningMode::Normal,
                None,
            )
            .await
        {
            Ok((mut statistics, partitions, _)) => {
                statistics.partitions_total = 0;
                state
                    .ctx
                    .set_pruned_partitions_stats(state.plan_id, statistics);
                for part in partitions.partitions {
                    // the query may be killed or finished early; stop quietly
                    if state.part_info_tx.send(Ok(part)).await.is_err() {
                        break;
                    }
                }
            }
            Err(err) => {
                let _ = state.part_info_tx.send(Err(err)).await;
            }
        }
        Ok(None)
    }
}
