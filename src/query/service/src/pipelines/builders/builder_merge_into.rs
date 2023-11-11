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

use common_base::base::tokio::sync::Semaphore;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::ROW_NUMBER_COL_NAME;
use common_pipeline_core::processors::InputPort;
use common_pipeline_core::processors::OutputPort;
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_core::Pipe;
use common_pipeline_core::PipeItem;
use common_pipeline_transforms::processors::create_dummy_item;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::executor::physical_plans::MergeInto;
use common_sql::executor::physical_plans::MergeIntoAddRowNumber;
use common_sql::executor::physical_plans::MergeIntoAppendNotMatched;
use common_sql::executor::physical_plans::MergeIntoSource;
use common_storages_fuse::operations::common::TransformSerializeSegment;
use common_storages_fuse::operations::MatchedSplitProcessor;
use common_storages_fuse::operations::MergeIntoNotMatchedProcessor;
use common_storages_fuse::operations::MergeIntoSplitProcessor;
use common_storages_fuse::operations::RowNumberAndLogSplitProcessor;
use common_storages_fuse::operations::TransformAddRowNumberColumnProcessor;
use common_storages_fuse::operations::TransformSerializeBlock;
use common_storages_fuse::FuseTable;

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
        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformAddRowNumberColumnProcessor::create(
                    transform_input_port,
                    transform_output_port,
                    node_index,
                    row_number.clone(),
                )
            })?;
        Ok(())
    }

    // build merge into append not matched pipeline.
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
        } = merge_into_append_not_macted;
        // receive row numbers and MutationLogs
        self.build_pipeline(input)?;
        self.main_pipeline.try_resize(1)?;

        assert!(self.join_state.is_some());
        assert!(self.probe_data_fields.is_some());

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
                self.probe_data_fields.clone().unwrap(),
            )?
            .into_pipe_item(),
            create_dummy_item(),
        ];
        self.main_pipeline.add_pipe(Pipe::create(2, 2, pipe_items));

        // not macthed operation
        let merge_into_not_matched_processor = MergeIntoNotMatchedProcessor::create(
            unmatched.clone(),
            input_schema.clone(),
            self.func_ctx.clone(),
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
        let tbl = self
            .ctx
            .build_table_by_table_info(catalog_info, table_info, None)?;
        // 1.fill default columns
        let table_default_schema = &tbl.schema().remove_computed_fields();
        let mut builder = self.main_pipeline.add_transform_with_specified_len(
            |transform_input_port, transform_output_port| {
                TransformResortAddOnWithoutSourceSchema::try_create(
                    self.ctx.clone(),
                    transform_input_port,
                    transform_output_port,
                    Arc::new(DataSchema::from(table_default_schema)),
                    tbl.clone(),
                )
            },
            1,
        )?;
        builder.add_items(vec![create_dummy_item()]);
        self.main_pipeline.add_pipe(builder.finalize());

        // 2.fill computed columns
        let table_computed_schema = &tbl.schema().remove_virtual_computed_fields();
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
                1,
            )?;
            builder.add_items(vec![create_dummy_item()]);
            self.main_pipeline.add_pipe(builder.finalize());
        }

        // 3. cluster sort
        let table = FuseTable::try_from_table(tbl.as_ref())?;
        let block_thresholds = table.get_block_thresholds();
        table.cluster_gen_for_append_with_specified_len(
            self.ctx.clone(),
            &mut self.main_pipeline,
            block_thresholds,
            1,
            1,
        )?;

        // 4. serialize block
        let cluster_stats_gen =
            table.get_cluster_stats_gen(self.ctx.clone(), 0, block_thresholds, None)?;
        let serialize_block_transform = TransformSerializeBlock::try_create(
            self.ctx.clone(),
            InputPort::create(),
            OutputPort::create(),
            table,
            cluster_stats_gen,
        )?;

        let pipe_items = vec![
            serialize_block_transform.into_pipe_item(),
            create_dummy_item(),
        ];
        self.main_pipeline.add_pipe(Pipe::create(2, 2, pipe_items));

        // 5. serialize segment
        let serialize_segment_transform = TransformSerializeSegment::new(
            self.ctx.clone(),
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

        Ok(())
    }

    pub(crate) fn build_merge_into_source(
        &mut self,
        merge_into_source: &MergeIntoSource,
    ) -> Result<()> {
        let MergeIntoSource { input, row_id_idx } = merge_into_source;

        self.build_pipeline(input)?;
        // merge into's parallism depends on the join probe number.
        let mut items = Vec::with_capacity(self.main_pipeline.output_len());
        let output_len = self.main_pipeline.output_len();
        for _ in 0..output_len {
            let merge_into_split_processor = MergeIntoSplitProcessor::create(*row_id_idx, false)?;
            items.push(merge_into_split_processor.into_pipe_item());
        }

        self.main_pipeline
            .add_pipe(Pipe::create(output_len, output_len * 2, items));

        Ok(())
    }

    // build merge into pipeline.
    pub(crate) fn build_merge_into(&mut self, merge_into: &MergeInto) -> Result<()> {
        let MergeInto {
            input,
            table_info,
            catalog_info,
            unmatched,
            matched,
            field_index_of_input_schema,
            row_id_idx,
            segments,
            distributed,
            ..
        } = merge_into;

        self.build_pipeline(input)?;

        let tbl = self
            .ctx
            .build_table_by_table_info(catalog_info, table_info, None)?;

        let table = FuseTable::try_from_table(tbl.as_ref())?;
        let block_thresholds = table.get_block_thresholds();

        let cluster_stats_gen =
            table.get_cluster_stats_gen(self.ctx.clone(), 0, block_thresholds, None)?;

        // this TransformSerializeBlock is just used to get block_builder
        let block_builder = TransformSerializeBlock::try_create(
            self.ctx.clone(),
            InputPort::create(),
            OutputPort::create(),
            table,
            cluster_stats_gen.clone(),
        )?
        .get_block_builder();

        let serialize_segment_transform = TransformSerializeSegment::new(
            self.ctx.clone(),
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
        // Handle matched and unmatched data separately.

        //                                                                                 +-----------------------------+-+
        //                                    +-----------------------+     Matched        |                             +-+
        //                                    |                       +---+--------------->|    MatchedSplitProcessor    |
        //                                    |                       |   |                |                             +-+
        // +----------------------+           |                       +---+                +-----------------------------+-+
        // |   MergeIntoSource    +---------->|MergeIntoSplitProcessor|
        // +----------------------+           |                       +---+                +-----------------------------+
        //                                    |                       |   | NotMatched     |                             +-+
        //                                    |                       +---+--------------->| MergeIntoNotMatchedProcessor| |
        //                                    +-----------------------+                    |                             +-+
        //                                                                                 +-----------------------------+
        // Note: here the output_port of MatchedSplitProcessor are arranged in the following order
        // (0) -> output_port_row_id
        // (1) -> output_port_updated

        // Outputs from MatchedSplitProcessor's output_port_updated and MergeIntoNotMatchedProcessor's output_port are merged and processed uniformly by the subsequent ResizeProcessor

        // receive matched data and not matched data parallelly.
        let mut pipe_items = Vec::with_capacity(self.main_pipeline.output_len());
        for _ in (0..self.main_pipeline.output_len()).step_by(2) {
            // Todo(JackTan25): We should optimize pipeline. when only not matched, we should ignore this
            let matched_split_processor = MatchedSplitProcessor::create(
                self.ctx.clone(),
                *row_id_idx,
                matched.clone(),
                field_index_of_input_schema.clone(),
                input.output_schema()?,
                Arc::new(DataSchema::from(tbl.schema())),
            )?;

            pipe_items.push(matched_split_processor.into_pipe_item());
            if !*distributed {
                // Todo(JackTan25): We should optimize pipeline. when only matched,we should ignore this
                let merge_into_not_matched_processor = MergeIntoNotMatchedProcessor::create(
                    unmatched.clone(),
                    input.output_schema()?,
                    self.func_ctx.clone(),
                )?;

                pipe_items.push(merge_into_not_matched_processor.into_pipe_item());
            } else {
                let input_num_columns = input.output_schema()?.num_fields();
                assert_eq!(
                    input.output_schema()?.field(input_num_columns - 1).name(),
                    ROW_NUMBER_COL_NAME
                );
                let input_port = InputPort::create();
                let output_port = OutputPort::create();
                // project row number column
                let proc = ProcessorPtr::create(CompoundBlockOperator::create(
                    input_port.clone(),
                    output_port.clone(),
                    input_num_columns,
                    self.func_ctx.clone(),
                    vec![BlockOperator::Project {
                        projection: vec![input_num_columns - 1],
                    }],
                ));
                pipe_items.push(PipeItem {
                    processor: proc,
                    inputs_port: vec![input_port],
                    outputs_port: vec![output_port],
                })
            };
        }
        self.main_pipeline.add_pipe(Pipe::create(
            self.main_pipeline.output_len(),
            get_output_len(&pipe_items),
            pipe_items.clone(),
        ));

        // row_id port0_1
        // matched update data port0_2
        // not macthed insert data port0_3
        // row_id port1_1
        // matched update data port1_2
        // not macthed insert data port1_3
        // ......
        assert_eq!(self.main_pipeline.output_len() % 3, 0);
        let mut ranges = Vec::with_capacity(self.main_pipeline.output_len() / 3 * 2);
        if !*distributed {
            // merge rowid and serialize_blocks
            for idx in (0..self.main_pipeline.output_len()).step_by(3) {
                ranges.push(vec![idx]);
                ranges.push(vec![idx + 1, idx + 2]);
            }
            self.main_pipeline.resize_partial_one(ranges.clone())?;
            assert_eq!(self.main_pipeline.output_len() % 2, 0);

            // shuffle outputs and resize row_id
            // ----------------------------------------------------------------------
            // row_id port0_1               row_id port0_1
            // data port0_2                 row_id port1_1              row_id port
            // row_id port1_1   ======>     ......           ======>
            // data port1_2                 data port0_2                data port0
            // ......                       data port1_2                data port1
            // ......                       .....                       .....
            // ----------------------------------------------------------------------
            let mut rules = Vec::with_capacity(self.main_pipeline.output_len());
            for idx in 0..(self.main_pipeline.output_len() / 2) {
                rules.push(idx);
                rules.push(idx + self.main_pipeline.output_len() / 2);
            }
            self.main_pipeline.reorder_inputs(rules);
            // resize row_id
            ranges.clear();
            let mut vec = Vec::with_capacity(self.main_pipeline.output_len() / 2);
            for idx in 0..(self.main_pipeline.output_len() / 2) {
                vec.push(idx);
            }
            ranges.push(vec.clone());
            for idx in 0..(self.main_pipeline.output_len() / 2) {
                ranges.push(vec![idx + self.main_pipeline.output_len() / 2]);
            }
            self.main_pipeline.resize_partial_one(ranges.clone())?;
        } else {
            // shuffle outputs and resize row_id
            // ----------------------------------------------------------------------
            // row_id port0_1              row_id port0_1              row_id port
            // matched data port0_2        row_id port1_1            matched data port0_2
            // row_number port0_3          matched data port0_2      matched data port1_2
            // row_id port1_1              matched data port1_2          ......
            // matched data port1_2  ===>       .....           ====>    ......
            // row_number port1_2               .....                    ......
            // row_number port0_3        row_number port0_3          row_number port
            // ......                    row_number port1_3
            // ......                           .....
            // ----------------------------------------------------------------------
            // do shuffle
            let mut rules = Vec::with_capacity(self.main_pipeline.output_len());
            for idx in 0..(self.main_pipeline.output_len() / 3) {
                rules.push(idx);
                rules.push(idx + self.main_pipeline.output_len() / 3);
                rules.push(idx + self.main_pipeline.output_len() / 3 * 2);
            }
            self.main_pipeline.reorder_inputs(rules);

            // resize row_id
            ranges.clear();
            let mut vec = Vec::with_capacity(self.main_pipeline.output_len() / 3);
            for idx in 0..(self.main_pipeline.output_len() / 3) {
                vec.push(idx);
            }
            ranges.push(vec.clone());
            for idx in 0..(self.main_pipeline.output_len() / 3) {
                ranges.push(vec![idx + self.main_pipeline.output_len() / 3]);
            }
            vec.clear();
            for idx in 0..(self.main_pipeline.output_len() / 3) {
                vec.push(idx + self.main_pipeline.output_len() / 3 * 2);
            }
            ranges.push(vec);
            self.main_pipeline.resize_partial_one(ranges.clone())?;
        }

        let fill_default_len = if !*distributed {
            // remove first row_id port
            self.main_pipeline.output_len() - 1
        } else {
            // remove first row_id port and last row_number_port
            self.main_pipeline.output_len() - 2
        };
        // fill default columns
        let table_default_schema = &table.schema().remove_computed_fields();
        let mut builder = self.main_pipeline.add_transform_with_specified_len(
            |transform_input_port, transform_output_port| {
                TransformResortAddOnWithoutSourceSchema::try_create(
                    self.ctx.clone(),
                    transform_input_port,
                    transform_output_port,
                    Arc::new(DataSchema::from(table_default_schema)),
                    tbl.clone(),
                )
            },
            fill_default_len,
        )?;

        if !*distributed {
            builder.add_items_prepend(vec![create_dummy_item()]);
            self.main_pipeline.add_pipe(builder.finalize());
        } else {
            builder.add_items_prepend(vec![create_dummy_item()]);
            // receive row_number
            builder.add_items(vec![create_dummy_item()]);
            self.main_pipeline.add_pipe(builder.finalize());
        }

        let fill_computed_len = if !*distributed {
            // remove first row_id port
            self.main_pipeline.output_len() - 1
        } else {
            // remove first row_id port and last row_number_port
            self.main_pipeline.output_len() - 2
        };

        // fill computed columns
        let table_computed_schema = &table.schema().remove_virtual_computed_fields();
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
                fill_computed_len,
            )?;
            if !*distributed {
                builder.add_items_prepend(vec![create_dummy_item()]);
                self.main_pipeline.add_pipe(builder.finalize());
            } else {
                builder.add_items_prepend(vec![create_dummy_item()]);
                // receive row_number
                builder.add_items(vec![create_dummy_item()]);
                self.main_pipeline.add_pipe(builder.finalize());
            }
        }

        let max_threads = self.settings.get_max_threads()?;
        let io_request_semaphore = Arc::new(Semaphore::new(max_threads as usize));

        // after filling default columns, we need to add cluster‘s blocksort if it's a cluster table
        let output_lens = self.main_pipeline.output_len();
        if !*distributed {
            table.cluster_gen_for_append_with_specified_len(
                self.ctx.clone(),
                &mut self.main_pipeline,
                block_thresholds,
                output_lens - 1,
                0,
            )?;
        } else {
            table.cluster_gen_for_append_with_specified_len(
                self.ctx.clone(),
                &mut self.main_pipeline,
                block_thresholds,
                output_lens - 2,
                1,
            )?;
        }

        pipe_items.clear();

        pipe_items.push(table.rowid_aggregate_mutator(
            self.ctx.clone(),
            block_builder,
            io_request_semaphore,
            segments.clone(),
        )?);

        let serialize_len = if !*distributed {
            self.main_pipeline.output_len() - 1
        } else {
            self.main_pipeline.output_len() - 2
        };

        for _ in 0..serialize_len {
            let serialize_block_transform = TransformSerializeBlock::try_create(
                self.ctx.clone(),
                InputPort::create(),
                OutputPort::create(),
                table,
                cluster_stats_gen.clone(),
            )?;
            pipe_items.push(serialize_block_transform.into_pipe_item());
        }

        // receive row_number
        if *distributed {
            pipe_items.push(create_dummy_item());
        }

        self.main_pipeline.add_pipe(Pipe::create(
            self.main_pipeline.output_len(),
            get_output_len(&pipe_items),
            pipe_items,
        ));

        // resize block ports
        // aggregate_mutator port/dummy_item port               aggregate_mutator port/ dummy_item (this depends on apply_row_id)
        // serialize_block port0                    ======>
        // serialize_block port1                                serialize_block port
        // .......
        // row_number_port (distributed)                        row_number_port(distributed)
        ranges.clear();
        ranges.push(vec![0]);
        let mut vec = Vec::with_capacity(self.main_pipeline.output_len());
        let output_lens = if !*distributed {
            self.main_pipeline.output_len() - 1
        } else {
            self.main_pipeline.output_len() - 2
        };
        for idx in 0..output_lens {
            vec.push(idx + 1);
        }
        ranges.push(vec);
        if *distributed {
            ranges.push(vec![self.main_pipeline.output_len() - 1]);
        }

        self.main_pipeline.resize_partial_one(ranges)?;

        let pipe_items = if !distributed {
            vec![
                create_dummy_item(),
                serialize_segment_transform.into_pipe_item(),
            ]
        } else {
            vec![
                create_dummy_item(),
                serialize_segment_transform.into_pipe_item(),
                create_dummy_item(),
            ]
        };

        // distributed: false
        //      output_port0: MutationLogs
        //      output_port1: MutationLogs
        //
        // distributed: true
        //      output_port0: MutationLogs
        //      output_port1: MutationLogs
        //      output_port2: row_numbers
        self.main_pipeline.add_pipe(Pipe::create(
            self.main_pipeline.output_len(),
            get_output_len(&pipe_items),
            pipe_items,
        ));

        // accumulate row_number
        if *distributed {
            let pipe_items = vec![
                create_dummy_item(),
                create_dummy_item(),
                AccumulateRowNumber::create()?.into_pipe_item(),
            ];
            self.main_pipeline.add_pipe(Pipe::create(
                self.main_pipeline.output_len(),
                get_output_len(&pipe_items),
                pipe_items,
            ));
        }

        Ok(())
    }
}
