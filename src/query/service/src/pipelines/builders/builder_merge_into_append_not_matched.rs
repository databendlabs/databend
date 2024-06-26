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
use databend_common_sql::binder::MergeIntoType;
use databend_common_sql::executor::physical_plans::MergeIntoAppendNotMatched;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::operations::MergeIntoNotMatchedProcessor;
use databend_common_storages_fuse::operations::RowNumberAndLogSplitProcessor;
use databend_common_storages_fuse::operations::TransformDistributedMergeIntoBlockDeserialize;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::operations::TransformSerializeSegment;
use databend_common_storages_fuse::FuseTable;

use crate::pipelines::processors::transforms::ExtractHashTableByRowNumber;
use crate::pipelines::processors::DeduplicateRowNumber;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    // build merge into append not matched pipeline.
    // it must be distributed merge into execution
    pub(crate) fn build_merge_into_append_not_matched(
        &mut self,
        merge_into_append_not_macted: &MergeIntoAppendNotMatched,
    ) -> Result<()> {
        self.build_pipeline(&merge_into_append_not_macted.input)?;
        // there are two cases:
        // 1. if source is build side (change_join_order = false).
        //  receive row numbers and MutationLogs, exactly below:
        //  1.1 full operation: MutationLogs and row numbers
        //  1.2 matched only: MutationLogs
        //  1.3 insert only: row numbers
        // 2. if target table is build side (change_join_order = true).
        //  receive rowids and MutationLogs,exactly below:
        //  2.1 full operation: rowids and MutationLogs
        //  2.2 matched only: rowids and MutationLogs
        //  2.3 insert only: MutationLogs

        // deserialize MixRowIdKindAndLog
        if merge_into_append_not_macted.change_join_order {
            self.main_pipeline
                .add_transform(|transform_input_port, transform_output_port| {
                    Ok(TransformDistributedMergeIntoBlockDeserialize::create(
                        transform_input_port,
                        transform_output_port,
                    ))
                })?;
        }

        let tbl = self
            .ctx
            .build_table_by_table_info(&merge_into_append_not_macted.table_info, None)?;
        let table = FuseTable::try_from_table(tbl.as_ref())?;
        let block_thresholds = table.get_block_thresholds();
        let cluster_stats_gen =
            table.get_cluster_stats_gen(self.ctx.clone(), 0, block_thresholds, None)?;

        // case 1
        if !merge_into_append_not_macted.change_join_order {
            if matches!(
                merge_into_append_not_macted.merge_type,
                MergeIntoType::MatchedOnly
            ) {
                // we will receive MutationLogs only without row_number.
                return Ok(());
            }
            assert!(self.join_state.is_some());
            assert!(self.merge_into_probe_data_fields.is_some());
            self.main_pipeline.resize(1, false)?;
            let join_state = self.join_state.clone().unwrap();
            // split row_number and log
            //      output_port_row_number
            //      output_port_log
            self.main_pipeline
                .add_pipe(RowNumberAndLogSplitProcessor::create()?.into_pipe());

            // accumulate source data which is not matched from hashstate
            let pipe_items = vec![
                DeduplicateRowNumber::create()?.into_pipe_item(),
                create_dummy_item(),
            ];
            self.main_pipeline.add_pipe(Pipe::create(2, 2, pipe_items));

            let pipe_items = vec![
                ExtractHashTableByRowNumber::create(
                    join_state,
                    self.merge_into_probe_data_fields.clone().unwrap(),
                    merge_into_append_not_macted.merge_type.clone(),
                )?
                .into_pipe_item(),
                create_dummy_item(),
            ];
            self.main_pipeline.add_pipe(Pipe::create(2, 2, pipe_items));

            // not matched operation
            let merge_into_not_matched_processor = MergeIntoNotMatchedProcessor::create(
                merge_into_append_not_macted.unmatched.clone(),
                merge_into_append_not_macted.input_schema.clone(),
                self.func_ctx.clone(),
                self.ctx.clone(),
            )?;
            let pipe_items = vec![
                merge_into_not_matched_processor.into_pipe_item(),
                create_dummy_item(),
            ];
            self.main_pipeline.add_pipe(Pipe::create(2, 2, pipe_items));

            // split row_number and log
            //      output_port_not_matched_data
            //      output_port_log
            // start to append data
            // 1. fill default and computed columns
            self.build_fill_columns_in_merge_into(
                tbl.clone(),
                1,
                true,
                false,
                false,
                false,
                merge_into_append_not_macted.unmatched.clone(),
            )?;

            // 2. compact blocks and cluster sort
            self.build_compact_and_cluster_sort_in_merge_into(table, true, false, false, 1, 1)?;

            // 3. serialize block
            let serialize_block_transform = TransformSerializeBlock::try_create(
                self.ctx.clone(),
                InputPort::create(),
                OutputPort::create(),
                table,
                cluster_stats_gen.clone(),
                MutationKind::MergeInto,
            )?;

            let pipe_items = vec![
                serialize_block_transform.into_pipe_item(),
                create_dummy_item(),
            ];
            self.main_pipeline.add_pipe(Pipe::create(2, 2, pipe_items));

            // 4. serialize segment
            let serialize_segment_transform = TransformSerializeSegment::new(
                InputPort::create(),
                OutputPort::create(),
                table,
                block_thresholds,
            );
            let pipe_items = vec![
                serialize_segment_transform.into_pipe_item(),
                create_dummy_item(),
            ];
            self.main_pipeline.add_pipe(Pipe::create(2, 2, pipe_items));

            // resize to one, because they are all mutation logs now.
            self.main_pipeline.try_resize(1)?;
        } else {
            // case 2
            if matches!(
                merge_into_append_not_macted.merge_type,
                MergeIntoType::InsertOnly
            ) {
                // we will receive MutationLogs only without rowids.
                return Ok(());
            }
            self.main_pipeline.resize(1, false)?;
            // we will receive MutationLogs and rowids. So we should apply
            // rowids firstly and then send all mutation logs to commit sink.
            // we need to spilt rowid and mutationlogs, and we can get pipeitems:
            //  1.row_id port
            //  2.logs port
            self.main_pipeline
                .add_pipe(RowNumberAndLogSplitProcessor::create()?.into_pipe());

            let max_threads = self.settings.get_max_threads()?;
            let io_request_semaphore = Arc::new(Semaphore::new(max_threads as usize));
            // MutationsLogs port0
            // MutationsLogs port1
            assert_eq!(self.main_pipeline.output_len(), 2);
            self.main_pipeline.add_pipe(Pipe::create(2, 2, vec![
                table.rowid_aggregate_mutator(
                    self.ctx.clone(),
                    cluster_stats_gen,
                    io_request_semaphore,
                    merge_into_append_not_macted.segments.clone(),
                    false, // we don't support for distributed mode.
                )?,
                create_dummy_item(),
            ]));
            assert_eq!(self.main_pipeline.output_len(), 2);
            self.main_pipeline.try_resize(1)?;
        }

        Ok(())
    }
}
