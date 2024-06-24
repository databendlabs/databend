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

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::TransformPipeBuilder;
use databend_common_pipeline_transforms::processors::create_dummy_item;
use databend_common_pipeline_transforms::processors::BlockCompactor;
use databend_common_pipeline_transforms::processors::TransformCompact;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::binder::MergeIntoType;
use databend_common_sql::executor::physical_plans::MergeInto;
use databend_common_sql::executor::physical_plans::MergeIntoAddRowNumber;
use databend_common_sql::executor::physical_plans::MergeIntoAppendNotMatched;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::operations::MergeIntoNotMatchedProcessor;
use databend_common_storages_fuse::operations::RowNumberAndLogSplitProcessor;
use databend_common_storages_fuse::operations::TransformAddRowNumberColumnProcessor;
use databend_common_storages_fuse::operations::TransformDistributedMergeIntoBlockDeserialize;
use databend_common_storages_fuse::operations::TransformDistributedMergeIntoBlockSerialize;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::operations::TransformSerializeSegment;
use databend_common_storages_fuse::operations::UnMatchedExprs;
use databend_common_storages_fuse::FuseTable;

use crate::pipelines::processors::transforms::AccumulateRowNumber;
use crate::pipelines::processors::transforms::ExtractHashTableByRowNumber;
use crate::pipelines::processors::transforms::TransformAddComputedColumns;
use crate::pipelines::processors::DeduplicateRowNumber;
use crate::pipelines::processors::TransformResortAddOnWithoutSourceSchema;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    // Build and add row_number column
    pub(crate) fn build_add_row_number(
        &mut self,
        add_row_number: &MergeIntoAddRowNumber,
    ) -> Result<()> {
        // it must be distributed merge into execution
        self.build_pipeline(&add_row_number.input)?;
        let node_index = add_row_number
            .cluster_index
            .get(&self.ctx.get_cluster().local_id);
        if node_index.is_none() {
            return Err(ErrorCode::NotFoundClusterNode(format!(
                "can't find out {} when build distributed merge into pipeline",
                self.ctx.get_cluster().local_id
            )));
        }
        let node_index = *node_index.unwrap() as u16;
        let row_number = Arc::new(AtomicU64::new(0));
        self.main_pipeline.add_transformer(|| {
            TransformAddRowNumberColumnProcessor::new(node_index, row_number.clone())
        });
        Ok(())
    }

    // build merge into append not matched pipeline.
    // it must be distributed merge into execution
    pub(crate) fn build_merge_into_append_not_matched(
        &mut self,
        merge_into_append_not_macted: &MergeIntoAppendNotMatched,
    ) -> Result<()> {
        let MergeIntoAppendNotMatched {
            input,
            table_info,
            catalog_info,
            unmatched,
            input_schema,
            merge_type,
            change_join_order,
            segments,
            ..
        } = merge_into_append_not_macted;

        // there are two cases:
        // 1. if source is build side (change_join_order = false).
        //  receive row numbers and MutationLogs, exactly below:
        //  1.1 full operation: row numbers and MutationLogs
        //  1.2 matched only: MutationLogs
        //  1.3 insert only: row numbers
        // 2. if target table is build side (change_join_order = true).
        //  receive rowids and MutationLogs,exactly below:
        //  2.1 full operation: rowids and MutationLogs
        //  2.2 matched only: rowids and MutationLogs
        //  2.3 insert only: MutationLogs
        self.build_pipeline(input)?;
        let change_join_order = *change_join_order;

        // deserialize MixRowIdKindAndLog
        if change_join_order {
            self.main_pipeline
                .add_transformer(|| TransformDistributedMergeIntoBlockDeserialize {});
        }

        let tbl = self
            .ctx
            .build_table_by_table_info(catalog_info, table_info, None)?;
        let table = FuseTable::try_from_table(tbl.as_ref())?;
        let block_thresholds = table.get_block_thresholds();

        // case 1
        if !change_join_order {
            if matches!(merge_type, MergeIntoType::MatchedOnly) {
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
                    merge_type.clone(),
                )?
                .into_pipe_item(),
                create_dummy_item(),
            ];
            self.main_pipeline.add_pipe(Pipe::create(2, 2, pipe_items));

            // not matched operation
            let merge_into_not_matched_processor = MergeIntoNotMatchedProcessor::create(
                unmatched.clone(),
                input_schema.clone(),
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
                unmatched.clone(),
            )?;

            // 2. compact blocks and cluster sort
            self.build_compact_and_cluster_sort_in_merge_into(table, true, false, false, 1, 1)?;

            // 3. serialize block
            let cluster_stats_gen =
                table.get_cluster_stats_gen(self.ctx.clone(), 0, block_thresholds, None)?;
            let serialize_block_transform = TransformSerializeBlock::try_create(
                self.ctx.clone(),
                InputPort::create(),
                OutputPort::create(),
                table,
                cluster_stats_gen,
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
            if matches!(merge_type, MergeIntoType::InsertOnly) {
                // we will receive MutationLogs only without rowids.
                return Ok(());
            }
            self.main_pipeline.resize(1, false)?;
            // we will receive MutationLogs and rowids. So we should apply
            // rowids firstly and then send all mutation logs to commit sink.
            // we need to spilt rowid and mutationlogs, and we can get pipeitems:
            //  1.rowid_port
            //  2.logs_port
            self.main_pipeline
                .add_pipe(RowNumberAndLogSplitProcessor::create()?.into_pipe());
            let cluster_stats_gen = table.get_cluster_stats_gen(
                self.ctx.clone(),
                0,
                table.get_block_thresholds(),
                None,
            )?;
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
                    segments.clone(),
                    false, // we don't support for distributed mode.
                )?,
                create_dummy_item(),
            ]));
            assert_eq!(self.main_pipeline.output_len(), 2);
            self.main_pipeline.try_resize(1)?;
        }

        Ok(())
    }

    fn resize_row_id(&mut self, step: usize) -> Result<()> {
        // resize row_id
        let mut ranges = Vec::with_capacity(self.main_pipeline.output_len());
        let mut vec = Vec::with_capacity(self.main_pipeline.output_len() / 3);
        for idx in 0..(self.main_pipeline.output_len() / step) {
            vec.push(idx);
        }
        ranges.push(vec.clone());
        for idx in 0..(self.main_pipeline.output_len() / step) {
            ranges.push(vec![idx + self.main_pipeline.output_len() / step]);
        }
        //  need to process row_number.
        if step == 3 {
            vec.clear();
            for idx in 0..(self.main_pipeline.output_len() / step) {
                vec.push(idx + self.main_pipeline.output_len() / step * 2);
            }
            ranges.push(vec);
        }

        self.main_pipeline.resize_partial_one(ranges.clone())
    }

    // build merge into pipeline.
    // the block rows is limitd by join (65536 rows), but we don't promise the block size.
    pub(crate) fn build_merge_into(&mut self, merge_into: &MergeInto) -> Result<()> {
        let MergeInto {
            input,
            table_info,
            catalog_info,
            unmatched,
            segments,
            distributed,
            merge_type,
            change_join_order,
            enable_right_broadcast,
            ..
        } = merge_into;
        let enable_right_broadcast = *enable_right_broadcast;
        let change_join_order = *change_join_order;
        let distributed = *distributed;
        self.build_pipeline(input)?;

        let tbl = self
            .ctx
            .build_table_by_table_info(catalog_info, table_info, None)?;

        let table = FuseTable::try_from_table(tbl.as_ref())?;
        let block_thresholds = table.get_block_thresholds();

        let cluster_stats_gen =
            table.get_cluster_stats_gen(self.ctx.clone(), 0, block_thresholds, None)?;

        let serialize_segment_transform = TransformSerializeSegment::new(
            InputPort::create(),
            OutputPort::create(),
            table,
            block_thresholds,
        );

        let get_output_len = |pipe_items: &Vec<PipeItem>| -> usize {
            let mut output_len = 0;
            for item in pipe_items.iter() {
                output_len += item.outputs_port.len();
            }
            output_len
        };

        let (_, need_match, need_unmatch) = match merge_type {
            MergeIntoType::FullOperation => (2, true, true),
            MergeIntoType::InsertOnly => (1, false, true),
            MergeIntoType::MatchedOnly => (1, true, false),
        };

        // the complete pipeline(with matched and unmatched) below:
        // row_id port0_1
        // matched update data port0_2
        // not matched insert data port0_3
        // row_id port1_1
        // matched update data port1_2
        // not matched insert data port1_3
        // ......
        // 1.for matched only, there are no not matched ports
        // 2.for unmatched only/insert only, there are no matched update ports and row_id ports
        let mut ranges = Vec::with_capacity(self.main_pipeline.output_len());
        if !distributed {
            // complete pipeline
            if need_match && need_unmatch {
                assert_eq!(self.main_pipeline.output_len() % 3, 0);
                // merge rowid and serialize_blocks
                for idx in (0..self.main_pipeline.output_len()).step_by(3) {
                    ranges.push(vec![idx]);
                    ranges.push(vec![idx + 1, idx + 2]);
                }
                self.main_pipeline.resize_partial_one(ranges.clone())?;
                assert_eq!(self.main_pipeline.output_len() % 2, 0);
            }

            // shuffle outputs and resize row_id
            // ----------------------------------------------------------------------
            // row_id port0_1               row_id port0_1
            // data port0_2                 row_id port1_1              row_id port
            // row_id port1_1   ======>     ......           ======>
            // data port1_2                 data port0_2                data port0
            // ......                       data port1_2                data port1
            // ......                       .....                       .....
            // ----------------------------------------------------------------------
            // the complete pipeline(with matched and unmatched) below:
            // 1. matched only or complete pipeline are same with above
            // 2. for unmatched only or insert only, there are no row_id ports
            if need_match {
                let mut rules = Vec::with_capacity(self.main_pipeline.output_len());
                for idx in 0..(self.main_pipeline.output_len() / 2) {
                    rules.push(idx);
                    rules.push(idx + self.main_pipeline.output_len() / 2);
                }
                self.main_pipeline.reorder_inputs(rules);
                // resize row_id
                self.resize_row_id(2)?;
            }
        } else {
            // I. if change_join_order is false, it means source is build side.
            // the complete pipeline(with matched and unmatched) below:
            // shuffle outputs and resize row_id
            // ----------------------------------------------------------------------
            // row_id port0_1              row_id port0_1              row_id port
            // matched data port0_2        row_id port1_1            matched data port0_2
            // row_number port0_3          matched data port0_2      matched data port1_2
            // row_id port1_1              matched data port1_2          ......
            // matched data port1_2  ===>       .....           ====>    ......
            // row_number port1_3               .....                    ......
            //                           row_number port0_3          row_number port
            // ......                    row_number port1_3
            // ......                           .....
            // ----------------------------------------------------------------------
            // 1.for matched only, there is no row_number
            // 2.for unmatched only/insert only, there is no row_id and matched data.
            // II. if change_join_order is true, it means target is build side.
            // the complete pipeline(with matched and unmatched) below:
            // shuffle outputs and resize row_id
            // ----------------------------------------------------------------------
            // row_id port0_1              row_id port0_1              row_id port
            // matched data port0_2        row_id port1_1            matched data port0_2
            // unmatched port0_3           matched data port0_2      matched data port1_2
            // row_id port1_1              matched data port1_2          ......
            // matched data port1_2  ===>       .....           ====>    ......
            // unmatched port1_3                .....                    ......
            //                            unmatched port0_3          unmatched port
            // ......                     unmatched port1_3
            // ......                           .....
            // ----------------------------------------------------------------------
            // 1.for matched only, there is no unmatched port
            // 2.for unmatched only/insert only, there is no row_id and matched data.
            // do shuffle
            let mut rules = Vec::with_capacity(self.main_pipeline.output_len());
            if need_match && need_unmatch {
                // matched and unmatched
                for idx in 0..(self.main_pipeline.output_len() / 3) {
                    rules.push(idx);
                    rules.push(idx + self.main_pipeline.output_len() / 3);
                    rules.push(idx + self.main_pipeline.output_len() / 3 * 2);
                }
                self.main_pipeline.reorder_inputs(rules);
                self.resize_row_id(3)?;
            } else if need_match && !need_unmatch {
                // matched only
                for idx in 0..(self.main_pipeline.output_len() / 2) {
                    rules.push(idx);
                    rules.push(idx + self.main_pipeline.output_len() / 2);
                }
                self.main_pipeline.reorder_inputs(rules);
                self.resize_row_id(2)?;
            } else if !need_match && need_unmatch {
                // insert-only, there are only row_numbers/unmatched ports
                self.main_pipeline.try_resize(1)?;
            }
        }

        let fill_default_len = if !distributed {
            if need_match {
                // remove first row_id port
                self.main_pipeline.output_len() - 1
            } else {
                self.main_pipeline.output_len()
            }
        } else if need_match && need_unmatch {
            // remove first row_id port and last row_number_port
            if enable_right_broadcast {
                self.main_pipeline.output_len() - 2
            } else {
                // remove first row_id port
                self.main_pipeline.output_len() - 1
            }
        } else if need_match && !need_unmatch {
            // remove first row_id port
            self.main_pipeline.output_len() - 1
        } else {
            // there are only row_number
            if enable_right_broadcast {
                0
            } else {
                // unmatched prot
                1
            }
        };

        // fill default and computed columns
        self.build_fill_columns_in_merge_into(
            tbl.clone(),
            fill_default_len,
            false,
            distributed,
            need_match,
            enable_right_broadcast,
            unmatched.clone(),
        )?;

        // after filling columns, we need to add cluster‘s blocksort if it's a cluster table
        let output_lens = self.main_pipeline.output_len();
        let serialize_len = if !distributed {
            let mid_len = if need_match {
                // with row_id
                output_lens - 1
            } else {
                // without row_id
                output_lens
            };
            self.build_compact_and_cluster_sort_in_merge_into(
                table, false, need_match, false, mid_len, 0,
            )?;
            mid_len
        } else {
            let (mid_len, last_len) = if need_match && need_unmatch {
                if !change_join_order {
                    // with row_id and row_number
                    (output_lens - 2, 1)
                } else {
                    // with row_id
                    (output_lens - 1, 0)
                }
            } else if change_join_order && !need_match && need_unmatch {
                // insert only
                (output_lens, 0)
            } else {
                // I. (with row_id and without row_number/unmatched) (need_match and !need_unmatch)
                // II. (without row_id and with row_number/unmatched) (!need_match and need_unmatch)
                // in fact for II, it should be (output_lens-1,1), but in this case, the
                // output_lens = 1, so it will be (0,1), and we just need to append a dummy_item.
                // but we use (output_lens - 1, 0) instead of (output_lens-1,1), because they will
                // arrive the same result (that's appending only one dummy item)
                (output_lens - 1, 0)
            };
            self.build_compact_and_cluster_sort_in_merge_into(
                table,
                false,
                need_match,
                enable_right_broadcast,
                mid_len,
                last_len,
            )?;
            mid_len
        };
        let max_threads = self.settings.get_max_threads()?;
        let io_request_semaphore = Arc::new(Semaphore::new(max_threads as usize));

        let mut pipe_items = Vec::with_capacity(self.main_pipeline.output_len());
        if need_match {
            // rowid should be accumulated in main node.
            if change_join_order && distributed {
                pipe_items.push(create_dummy_item())
            } else {
                pipe_items.push(table.rowid_aggregate_mutator(
                    self.ctx.clone(),
                    cluster_stats_gen.clone(),
                    io_request_semaphore,
                    segments.clone(),
                    merge_into.target_build_optimization,
                )?);
            }
        }

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

        // receive row_number
        if enable_right_broadcast {
            pipe_items.push(create_dummy_item());
        }

        self.main_pipeline.add_pipe(Pipe::create(
            self.main_pipeline.output_len(),
            get_output_len(&pipe_items),
            pipe_items,
        ));

        // complete pipeline(if change_join_order is true, there is no row_number_port):
        // resize block ports
        // aggregate_mutator port/dummy_item port               aggregate_mutator port/ dummy_item (this depends on apply_row_id)
        // serialize_block port0                    ======>
        // serialize_block port1                                serialize_block port
        // .......
        // row_number_port (distributed)                        row_number_port(distributed)
        ranges.clear();
        let offset = if need_match {
            ranges.push(vec![0]);
            1
        } else {
            0
        };

        // for distributed insert-only(right anti join), the serialize_len is zero.
        if serialize_len > 0 {
            let mut vec = Vec::with_capacity(self.main_pipeline.output_len());
            for idx in 0..serialize_len {
                vec.push(idx + offset);
            }

            ranges.push(vec);
        }

        // with row_number
        if enable_right_broadcast {
            ranges.push(vec![self.main_pipeline.output_len() - 1]);
        }

        self.main_pipeline.resize_partial_one(ranges)?;

        let pipe_items = if !distributed {
            let mut vec = Vec::with_capacity(2);
            if need_match {
                vec.push(create_dummy_item());
            }
            vec.push(serialize_segment_transform.into_pipe_item());
            vec
        } else {
            let mut vec = Vec::with_capacity(3);
            if need_match {
                vec.push(create_dummy_item())
            }
            // for distributed insert-only(right anti join), the serialize_len is zero.
            // and there is no serialize data here.
            if serialize_len > 0 {
                vec.push(serialize_segment_transform.into_pipe_item());
            }

            if enable_right_broadcast {
                vec.push(create_dummy_item())
            }
            vec
        };

        // the complete pipeline(with matched and unmatched) below:
        // I. change_join_order: false
        // distributed: false
        //      output_port0: MutationLogs
        //      output_port1: MutationLogs
        //
        // distributed: true
        //      output_port0: MutationLogs
        //      output_port1: MutationLogs
        //      output_port2: row_numbers
        // 1.for matched only, there are no row_numbers
        // 2.for insert only, there is no output_port0,output_port1.
        // II. change_join_order: true
        // distributed: false
        //      output_port0: MutationLogs
        //      output_port1: MutationLogs
        //
        // distributed: true
        //      output_port0: rowid
        //      output_port1: MutationLogs
        // 1.for insert only, there is no output_port0.
        self.main_pipeline.add_pipe(Pipe::create(
            self.main_pipeline.output_len(),
            get_output_len(&pipe_items),
            pipe_items,
        ));

        // accumulate row_number
        if enable_right_broadcast {
            let pipe_items = if need_match {
                vec![
                    create_dummy_item(),
                    create_dummy_item(),
                    AccumulateRowNumber::create()?.into_pipe_item(),
                ]
            } else {
                vec![AccumulateRowNumber::create()?.into_pipe_item()]
            };

            self.main_pipeline.add_pipe(Pipe::create(
                self.main_pipeline.output_len(),
                get_output_len(&pipe_items),
                pipe_items,
            ));
        }

        // add distributed_merge_into_block_serialize
        // we will wrap rowid and log as MixRowIdKindAndLog
        if distributed && change_join_order {
            self.main_pipeline
                .add_transformer(|| TransformDistributedMergeIntoBlockSerialize {});
        }
        Ok(())
    }

    fn build_fill_columns_in_merge_into(
        &mut self,
        tbl: Arc<dyn Table>,
        transform_len: usize,
        is_build_merge_into_append_not_matched: bool,
        distributed: bool,
        need_match: bool,
        enable_right_broadcast: bool,
        unmatched: UnMatchedExprs,
    ) -> Result<()> {
        let table = FuseTable::try_from_table(tbl.as_ref())?;

        let add_builder_pipe = |mut builder: TransformPipeBuilder| -> Pipe {
            if is_build_merge_into_append_not_matched {
                builder.add_items(vec![create_dummy_item()]);
            } else if !distributed {
                if need_match {
                    builder.add_items_prepend(vec![create_dummy_item()]);
                }
            } else {
                if need_match {
                    // receive row_id
                    builder.add_items_prepend(vec![create_dummy_item()]);
                }
                if enable_right_broadcast {
                    // receive row_number
                    builder.add_items(vec![create_dummy_item()]);
                }
            }
            builder.finalize()
        };

        // fill default columns
        let table_default_schema = &table.schema_with_stream().remove_computed_fields();
        let mut builder = self.main_pipeline.add_transform_with_specified_len(
            |transform_input_port, transform_output_port| {
                TransformResortAddOnWithoutSourceSchema::try_create(
                    self.ctx.clone(),
                    transform_input_port,
                    transform_output_port,
                    Arc::new(DataSchema::from(table_default_schema)),
                    unmatched.clone(),
                    tbl.clone(),
                    Arc::new(DataSchema::from(table.schema_with_stream())),
                )
            },
            transform_len,
        )?;
        self.main_pipeline.add_pipe(add_builder_pipe(builder));

        // fill computed columns
        let table_computed_schema = &table.schema_with_stream().remove_virtual_computed_fields();
        let default_schema: DataSchemaRef = Arc::new(table_default_schema.into());
        let computed_schema: DataSchemaRef = Arc::new(table_computed_schema.into());
        if default_schema != computed_schema {
            builder = self.main_pipeline.add_transform_with_specified_len(
                |transform_input_port, transform_output_port| {
                    TransformAddComputedColumns::try_create(
                        self.ctx.clone(),
                        transform_input_port,
                        transform_output_port,
                        default_schema.clone(),
                        computed_schema.clone(),
                    )
                },
                transform_len,
            )?;
            self.main_pipeline.add_pipe(add_builder_pipe(builder));
        }
        Ok(())
    }

    fn build_compact_and_cluster_sort_in_merge_into(
        &mut self,
        table: &FuseTable,
        is_build_merge_into_append_not_matched: bool,
        need_match: bool,
        enable_right_broadcast: bool,
        mid_len: usize,
        last_len: usize,
    ) -> Result<()> {
        let block_thresholds = table.get_block_thresholds();
        // we should avoid too much little block write, because for s3 write, there are too many
        // little blocks, it will cause high latency.
        let mut builder = self.main_pipeline.add_transform_with_specified_len(
            |transform_input_port, transform_output_port| {
                Ok(ProcessorPtr::create(TransformCompact::try_create(
                    transform_input_port,
                    transform_output_port,
                    BlockCompactor::new(block_thresholds),
                )?))
            },
            mid_len,
        )?;

        if is_build_merge_into_append_not_matched {
            builder.add_items(vec![create_dummy_item()]);
        }

        if need_match {
            builder.add_items_prepend(vec![create_dummy_item()]);
        }

        // need to receive row_number, we should give a dummy item here.
        if enable_right_broadcast {
            builder.add_items(vec![create_dummy_item()]);
        }
        self.main_pipeline.add_pipe(builder.finalize());

        // cluster sort
        table.cluster_gen_for_append_with_specified_len(
            self.ctx.clone(),
            &mut self.main_pipeline,
            block_thresholds,
            mid_len,
            last_len,
        )?;
        Ok(())
    }
}
