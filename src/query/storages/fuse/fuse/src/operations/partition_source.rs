//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_sources::processors::sources::AsyncSource;
use common_pipeline_sources::processors::sources::AsyncSourcer;

use crate::fuse_lazy_part::FuseLazyPartInfo;
use crate::FuseTable;

// Read partition from segment info.
// No output:
// ctx.try_set_partitions(partitions)?;
// The downstream usage:
// ctx.try_get_part();
pub struct PartitionSource {
    ctx: Arc<dyn TableContext>,
    plan: DataSourcePlan,
    fuse_table: FuseTable,
    finish: bool,
}

impl PartitionSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        plan: DataSourcePlan,
        fuse_table: FuseTable,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, PartitionSource {
            ctx,
            plan,
            fuse_table,
            finish: false,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for PartitionSource {
    const NAME: &'static str = "read_partition";

    #[async_trait::unboxed_simple]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }
        self.finish = true;

        let plan = self.plan.clone();
        let push_downs = plan.push_downs.clone();
        let table = self.fuse_table.clone();
        let table_info = table.table_info.clone();
        let table_ctx = self.ctx.clone();
        let op = table.operator.clone();

        let mut segments = Vec::with_capacity(plan.parts.len());
        for part in &plan.parts {
            if let Some(part_info) = part.as_any().downcast_ref::<FuseLazyPartInfo>() {
                segments.push(part_info.segment_location.clone());
            }
        }

        if !segments.is_empty() {
            let (_statistics, partitions) = table
                .prune_snapshot_blocks(table_ctx.clone(), op, push_downs, table_info, segments, 0)
                .await?;
            table_ctx.try_set_partitions(partitions)?;
        }

        Ok(None)
    }
}
