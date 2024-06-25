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
use databend_common_pipeline_core::TransformPipeBuilder;
use databend_common_pipeline_transforms::processors::create_dummy_item;
use databend_common_pipeline_transforms::processors::BlockCompactor;
use databend_common_pipeline_transforms::processors::TransformCompact;
use databend_common_sql::executor::physical_plans::MergeInto;
use databend_common_sql::executor::physical_plans::MergeIntoAddRowNumber;
use databend_common_storages_fuse::operations::TransformAddRowNumberColumnProcessor;
use databend_common_storages_fuse::operations::TransformDistributedMergeIntoBlockSerialize;
use databend_common_storages_fuse::operations::TransformSerializeSegment;
use databend_common_storages_fuse::operations::UnMatchedExprs;
use databend_common_storages_fuse::FuseTable;

use crate::pipelines::processors::transforms::AccumulateRowNumber;
use crate::pipelines::processors::transforms::TransformAddComputedColumns;
use crate::pipelines::processors::TransformResortAddOnWithoutSourceSchema;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    // build add row_number column for source table when enable_right_broadcast = true
    // it must be distributed merge into execution
    pub(crate) fn build_merge_into_add_row_number(
        &mut self,
        merge_into_add_row_number: &MergeIntoAddRowNumber,
    ) -> Result<()> {
        self.build_pipeline(&merge_into_add_row_number.input)?;

        let node_index = merge_into_add_row_number
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

    // build merge into mutation pipeline
    pub(crate) fn build_merge_into(&mut self, merge_into: &MergeInto) -> Result<()> {
        self.build_pipeline(&merge_into.input)?;

        let tbl = self.ctx.build_table_by_table_info(
            &merge_into.catalog_info,
            &merge_into.table_info,
            None,
        )?;

        let table = FuseTable::try_from_table(tbl.as_ref())?;
        let block_thresholds = table.get_block_thresholds();

        let serialize_segment_transform = TransformSerializeSegment::new(
            InputPort::create(),
            OutputPort::create(),
            table,
            block_thresholds,
        );

        // complete pipeline:
        // aggregate_mutator port/dummy_item port               aggregate_mutator port/dummy_item port (this depends on apply_row_id)
        // serialize_block port0
        // serialize_block port1                   ======>      serialize_block port
        // .......
        // row_number_port (enable_right_broadcast = true)      row_number_port
        let output_len = self.main_pipeline.output_len();
        let mut ranges = Vec::with_capacity(output_len);
        let (serialize_len, _) = merge_into
            .merge_into_op
            .get_serialize_and_row_number_len(output_len, merge_into.enable_right_broadcast);

        // row id port
        let row_id_offset = if merge_into.need_match {
            ranges.push(vec![0]);
            1
        } else {
            0
        };

        // resize data ports
        // for distributed insert-only(right anti join), the serialize_len is zero.
        if serialize_len > 0 {
            let mut vec = Vec::with_capacity(output_len);
            for idx in 0..serialize_len {
                vec.push(idx + row_id_offset);
            }
            ranges.push(vec);
        }

        // with row_number
        if merge_into.enable_right_broadcast {
            ranges.push(vec![output_len - 1]);
        }

        self.main_pipeline.resize_partial_one(ranges)?;

        let pipe_items = if !merge_into.distributed {
            let mut vec = Vec::with_capacity(2);
            if merge_into.need_match {
                // row_id port
                vec.push(create_dummy_item());
            }
            // data port
            vec.push(serialize_segment_transform.into_pipe_item());
            vec
        } else {
            let mut vec = Vec::with_capacity(3);
            if merge_into.need_match {
                // row_id port
                vec.push(create_dummy_item())
            }
            // for distributed insert-only(right anti join), the serialize_len is zero.
            if serialize_len > 0 {
                // data port
                vec.push(serialize_segment_transform.into_pipe_item());
            }
            if merge_into.enable_right_broadcast {
                // row number port
                vec.push(create_dummy_item())
            }
            vec
        };

        // the complete pipeline:
        // -----------Standalone-----------
        // output_port0: MutationLogs(row_id)
        // output_port1: MutationLogs(data)
        // 1. FullOperation and MatchedOnly: same as above
        // 2. InsertOnly: no output_port0

        //-----------Distributed-----------
        // output_port0: MutationLogs(row_id)
        // output_port1: MutationLogs(data)
        // output_port2: row_number (enable_right_broadcast = true)
        // 1. MatchedOnly, no output_port2
        // 2. InsertOnly: no output_port0

        let output_len = pipe_items.iter().map(|item| item.outputs_port.len()).sum();
        self.main_pipeline.add_pipe(Pipe::create(
            self.main_pipeline.output_len(),
            output_len,
            pipe_items,
        ));

        // accumulate row_number
        if merge_into.enable_right_broadcast {
            let pipe_items = if merge_into.need_match {
                vec![
                    create_dummy_item(),
                    create_dummy_item(),
                    AccumulateRowNumber::create()?.into_pipe_item(),
                ]
            } else {
                vec![AccumulateRowNumber::create()?.into_pipe_item()]
            };
            let output_len = pipe_items.iter().map(|item| item.outputs_port.len()).sum();
            self.main_pipeline.add_pipe(Pipe::create(
                self.main_pipeline.output_len(),
                output_len,
                pipe_items,
            ));
        }

        // add distributed_merge_into_block_serialize
        // we will wrap rowid and log as MixRowIdKindAndLog
        if merge_into.distributed && merge_into.change_join_order {
            self.main_pipeline
                .add_transform(|transform_input_port, transform_output_port| {
                    Ok(TransformDistributedMergeIntoBlockSerialize::create(
                        transform_input_port,
                        transform_output_port,
                    ))
                })?;
        }
        Ok(())
    }

    pub fn build_fill_columns_in_merge_into(
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

    pub fn build_compact_and_cluster_sort_in_merge_into(
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
