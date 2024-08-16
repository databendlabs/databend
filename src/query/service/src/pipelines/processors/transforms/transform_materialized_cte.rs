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

use databend_common_base::base::tokio::sync::Notify;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::Sink;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_sql::IndexType;
use parking_lot::Mutex;
use parking_lot::RwLock;

use crate::sessions::QueryContext;

pub struct MaterializedCteState {
    pub ctx: Arc<QueryContext>,
    pub left_sinker_count: Arc<RwLock<usize>>,
    pub sink_finished_notifier: Arc<Notify>,
    pub sink_finished: Arc<Mutex<bool>>,
}

impl MaterializedCteState {
    pub fn new(ctx: Arc<QueryContext>) -> Self {
        MaterializedCteState {
            ctx,
            left_sinker_count: Arc::new(RwLock::new(0)),
            sink_finished_notifier: Arc::new(Default::default()),
            sink_finished: Arc::new(Mutex::new(false)),
        }
    }

    pub fn attach_sinker(&self) -> Result<()> {
        let mut left_sinker_count = self.left_sinker_count.write();
        *left_sinker_count += 1;
        Ok(())
    }

    pub fn detach_sinker(&self, cte_idx: IndexType) -> Result<()> {
        let mut left_sinker_count = self.left_sinker_count.write();
        *left_sinker_count -= 1;
        if *left_sinker_count == 0 {
            // Sink finished. Clone materialized blocks to all materialized cte in ctx with same cte_idx
            let blocks = self.ctx.get_materialized_cte((cte_idx, 1usize))?;
            if let Some(blocks) = blocks {
                let blocks = (*blocks).read();
                let ctes = self.ctx.get_materialized_ctes();
                for (idx, cte) in ctes.write().iter() {
                    if idx.0 == cte_idx && idx.1 != 1 {
                        let mut cte = cte.write();
                        *cte = (*blocks).clone();
                    }
                }
            }
            let mut sink_finished = self.sink_finished.lock();
            *sink_finished = true;
            self.sink_finished_notifier.notify_waiters();
        }
        Ok(())
    }

    pub async fn wait_sink_finished(&self) -> Result<()> {
        let notified = {
            let sink_finished = self.sink_finished.lock();
            match *sink_finished {
                true => None,
                false => Some(self.sink_finished_notifier.notified()),
            }
        };

        if let Some(notified) = notified {
            notified.await;
        }
        Ok(())
    }
}

pub struct MaterializedCteSink {
    cte_idx: IndexType,
    ctx: Arc<QueryContext>,
    blocks: Vec<DataBlock>,
    state: Arc<MaterializedCteState>,
}

impl MaterializedCteSink {
    pub fn create(
        ctx: Arc<QueryContext>,
        cte_idx: IndexType,
        state: Arc<MaterializedCteState>,
    ) -> Result<Self> {
        state.attach_sinker()?;
        Ok(MaterializedCteSink {
            cte_idx,
            ctx,
            blocks: vec![],
            state,
        })
    }
}

impl Sink for MaterializedCteSink {
    const NAME: &'static str = "MaterializedCteSink";

    fn on_finish(&mut self) -> Result<()> {
        let materialized_cte = self.ctx.get_materialized_cte((self.cte_idx, 1usize))?;
        if let Some(blocks) = materialized_cte {
            let mut blocks = blocks.write();
            blocks.extend(self.blocks.clone());
        }
        self.state.detach_sinker(self.cte_idx)
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        self.blocks.push(data_block);
        Ok(())
    }
}

pub struct MaterializedCteSource {
    cte_idx: (IndexType, IndexType),
    ctx: Arc<QueryContext>,
    cte_state: Arc<MaterializedCteState>,
    offsets: Vec<IndexType>,
}

impl MaterializedCteSource {
    pub fn create(
        ctx: Arc<QueryContext>,
        output_port: Arc<OutputPort>,
        cte_idx: (IndexType, IndexType),
        cte_state: Arc<MaterializedCteState>,
        offsets: Vec<IndexType>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output_port, MaterializedCteSource {
            ctx,
            cte_idx,
            cte_state,
            offsets,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for MaterializedCteSource {
    const NAME: &'static str = "MaterializedCteSource";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        self.cte_state.wait_sink_finished().await?;
        let materialized_cte = self.ctx.get_materialized_cte(self.cte_idx)?;
        if let Some(blocks) = materialized_cte {
            let mut blocks_guard = blocks.write();
            let block = blocks_guard.pop();
            if let Some(b) = block {
                if self.offsets.len() == b.num_columns() {
                    return Ok(Some(b));
                }
                let row_len = b.num_rows();
                let pruned_columns = self
                    .offsets
                    .iter()
                    .map(|offset| b.get_by_offset(*offset).clone())
                    .collect::<Vec<BlockEntry>>();

                Ok(Some(DataBlock::new(pruned_columns, row_len)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
