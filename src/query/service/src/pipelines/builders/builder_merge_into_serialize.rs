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

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_transforms::processors::create_dummy_item;
use databend_common_sql::executor::physical_plans::MergeIntoSerialize;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::FuseTable;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_merge_into_serialize(
        &mut self,
        merge_into_serialize: &MergeIntoSerialize,
    ) -> Result<()> {
        self.build_pipeline(&merge_into_serialize.input)?;

        // For row_id port, if (distributed && !enable_right_broadcast) create dummy; else create rowid_aggregate_mutator
        // For matched data port and unmatched port, do serialize
        // For row_number port, create dummy

        let tbl = self.ctx.build_table_by_table_info(
            &merge_into_serialize.catalog_info,
            &merge_into_serialize.table_info,
            None,
        )?;

        let table = FuseTable::try_from_table(tbl.as_ref())?;
        let block_thresholds = table.get_block_thresholds();

        let cluster_stats_gen =
            table.get_cluster_stats_gen(self.ctx.clone(), 0, block_thresholds, None)?;

        let (serialize_len, row_number_len) = merge_into_serialize
            .merge_into_op
            .get_serialize_and_row_number_len(
                self.main_pipeline.output_len(),
                merge_into_serialize.enable_right_broadcast,
            );

        // 1. Fill default and computed columns
        self.build_fill_columns_in_merge_into(
            tbl.clone(),
            serialize_len,
            false,
            merge_into_serialize.distributed,
            merge_into_serialize.need_match,
            merge_into_serialize.enable_right_broadcast,
            merge_into_serialize.unmatched.clone(),
        )?;

        // 2. Add cluster‘s blocksort if it's a cluster table
        self.build_compact_and_cluster_sort_in_merge_into(
            table,
            false,
            merge_into_serialize.need_match,
            merge_into_serialize.enable_right_broadcast,
            serialize_len,
            row_number_len,
        )?;

        let max_threads = self.settings.get_max_threads()?;
        let io_request_semaphore = Arc::new(Semaphore::new(max_threads as usize));

        let mut pipe_items = Vec::with_capacity(self.main_pipeline.output_len());

        // 3.1 Add rowid_aggregate_mutator for row_id port
        if merge_into_serialize.need_match {
            // rowid should be accumulated in main node.
            if merge_into_serialize.distributed && merge_into_serialize.change_join_order {
                pipe_items.push(create_dummy_item())
            } else {
                pipe_items.push(table.rowid_aggregate_mutator(
                    self.ctx.clone(),
                    cluster_stats_gen.clone(),
                    io_request_semaphore,
                    merge_into_serialize.segments.clone(),
                    false,
                )?);
            }
        }

        // 3.2 Add serialize_block_transform for data port
        for _ in 0..serialize_len {
            let serialize_block_transform = TransformSerializeBlock::try_create(
                self.ctx.clone(),
                InputPort::create(),
                OutputPort::create(),
                table,
                cluster_stats_gen.clone(),
                MutationKind::MergeInto,
            )?;
            pipe_items.push(serialize_block_transform.into_pipe_item());
        }

        // 3.3 Add dummy port for row_number
        if merge_into_serialize.enable_right_broadcast {
            pipe_items.push(create_dummy_item());
        }

        let output_len = pipe_items.iter().map(|item| item.outputs_port.len()).sum();
        self.main_pipeline.add_pipe(Pipe::create(
            self.main_pipeline.output_len(),
            output_len,
            pipe_items,
        ));
        Ok(())
    }
}
