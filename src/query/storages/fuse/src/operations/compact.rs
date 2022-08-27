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

use common_exception::Result;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SinkPipeBuilder;
use common_pipeline_transforms::processors::transforms::TransformCompact;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;

use super::FuseTableSink;
use crate::operations::CompactMutator;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;
use crate::Table;
use crate::TableContext;
use crate::TableMutator;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;

impl FuseTable {
    pub(crate) async fn do_compact(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        pipeline: &mut Pipeline,
    ) -> Result<Option<Arc<dyn TableMutator>>> {
        let snapshot_opt = self.read_table_snapshot(ctx.clone()).await?;
        let base_snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no compaction.
            return Ok(None);
        };

        if base_snapshot.summary.block_count <= 1 {
            return Ok(None);
        }

        let block_compactor = self.get_block_compactor();
        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        let mut mutator = CompactMutator::try_create(
            ctx.clone(),
            base_snapshot,
            block_compactor.clone(),
            self.meta_location_generator().clone(),
            block_per_seg,
            self.cluster_key_meta.is_some(),
        )?;
        let need_compact = mutator.blocks_select().await?;
        if !need_compact {
            return Ok(None);
        }

        let partitions_total = mutator.partitions_total();
        let (statistics, parts) = self.read_partitions_with_metas(
            ctx.clone(),
            None,
            mutator.selected_blocks(),
            partitions_total,
        )?;
        let table_info = self.get_table_info();
        let description = statistics.get_description(table_info);
        let plan = ReadDataSourcePlan {
            catalog,
            source_info: SourceInfo::TableSource(table_info.clone()),
            scan_fields: None,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs: None,
        };

        ctx.try_set_partitions(plan.parts.clone())?;
        self.do_read2(ctx.clone(), &plan, pipeline)?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformCompact::try_create(
                transform_input_port,
                transform_output_port,
                block_compactor.to_compactor(),
            )
        })?;

        let mut sink_pipeline_builder = SinkPipeBuilder::create();
        for _ in 0..pipeline.output_len() {
            let input_port = InputPort::create();
            sink_pipeline_builder.add_sink(
                input_port.clone(),
                FuseTableSink::try_create(
                    input_port,
                    ctx.clone(),
                    block_per_seg,
                    mutator.get_storage_operator(),
                    self.meta_location_generator().clone(),
                    ClusterStatsGenerator::default(),
                )?,
            );
        }

        pipeline.add_pipe(sink_pipeline_builder.finalize());
        Ok(Some(Arc::new(mutator)))
    }
}
