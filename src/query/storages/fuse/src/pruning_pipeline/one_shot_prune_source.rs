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
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;

use crate::FuseTable;
use crate::SegmentLocation;

/// The pruning work of the `enable_prune_pipeline = 0` mode: the lazy
/// segments assigned to this node are pruned by one `prune_snapshot_blocks`
/// call instead of a streaming prune pipeline. Which node prunes which
/// segments is decided earlier, by fragment redistribution, and is the same
/// in both modes. Pruned parts go out over `part_info_tx`; what was scanned
/// is published under `plan_id` (`partitions_total` was published once by the
/// planner).
#[derive(Clone)]
pub struct OneShotPrune {
    pub ctx: Arc<dyn TableContext>,
    pub table: FuseTable,
    pub push_downs: Option<PushDownInfo>,
    pub lazy_init_segments: Vec<SegmentLocation>,
    pub part_info_tx: Sender<Result<PartInfoPtr>>,
    pub plan_id: u32,
}

impl OneShotPrune {
    /// Prunes and forwards the parts. A pruning error is sent down the
    /// channel for the scan side and also returned, so the processor fails
    /// too: `EXPLAIN` drops the receiver and only sees the pipeline result.
    async fn run(self) -> Result<()> {
        let table_schema = self.table.schema_with_stream();
        let pruned = self
            .table
            .prune_snapshot_blocks(
                self.ctx.clone(),
                self.push_downs,
                table_schema,
                self.lazy_init_segments,
                0,
                ReadPartitionsPruningMode::Normal,
                None,
            )
            .await;
        let (mut statistics, partitions, _) = match pruned {
            Ok(pruned) => pruned,
            Err(err) => {
                let _ = self.part_info_tx.send(Err(err.clone())).await;
                return Err(err);
            }
        };
        statistics.partitions_total = 0;
        self.ctx
            .set_pruned_partitions_stats(self.plan_id, statistics);
        for part in partitions.partitions {
            // the query may be killed or finished early; stop quietly
            if self.part_info_tx.send(Ok(part)).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

/// Runs [`OneShotPrune`] as a one-processor pipeline, so that every caller
/// that builds a prune pipeline — `TableScan`, `EXPLAIN`, `EXPLAIN ANALYZE` —
/// executes this mode the same way as the streaming one. Produces no blocks.
pub struct OneShotPruneSource {
    prune: Option<OneShotPrune>,
}

impl OneShotPruneSource {
    pub fn create(prune: OneShotPrune, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        AsyncSourcer::create(prune.ctx.get_scan_progress(), output, Self {
            prune: Some(prune),
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for OneShotPruneSource {
    const NAME: &'static str = "OneShotPruneSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if let Some(prune) = self.prune.take() {
            prune.run().await?;
        }
        Ok(None)
    }
}
