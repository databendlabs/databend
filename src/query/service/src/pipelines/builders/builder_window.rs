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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_sql::executor::physical_plans::Window;
use databend_common_sql::executor::physical_plans::WindowPartition;
use databend_common_storage::DataOperator;

use crate::pipelines::processors::transforms::FrameBound;
use crate::pipelines::processors::transforms::TransformWindowPartitionBucket;
use crate::pipelines::processors::transforms::TransformWindowPartitionScatter;
use crate::pipelines::processors::transforms::TransformWindowPartitionSort;
use crate::pipelines::processors::transforms::TransformWindowPartitionSpillReader;
use crate::pipelines::processors::transforms::TransformWindowPartitionSpillWriter;
use crate::pipelines::processors::transforms::WindowFunctionInfo;
use crate::pipelines::processors::TransformWindow;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_window(&mut self, window: &Window) -> Result<()> {
        self.build_pipeline(&window.input)?;

        let input_schema = window.input.output_schema()?;

        let partition_by = window
            .partition_by
            .iter()
            .map(|p| {
                let offset = input_schema.index_of(&p.to_string())?;
                Ok(offset)
            })
            .collect::<Result<Vec<_>>>()?;

        let order_by = window
            .order_by
            .iter()
            .map(|o| {
                let offset = input_schema.index_of(&o.order_by.to_string())?;
                Ok(SortColumnDescription {
                    offset,
                    asc: o.asc,
                    nulls_first: o.nulls_first,
                    is_nullable: input_schema.field(offset).is_nullable(), // Used for check null frame.
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let old_output_len = self.main_pipeline.output_len();
        // `TransformWindow` is a pipeline breaker.
        if partition_by.is_empty() {
            self.main_pipeline.try_resize(1)?;
        }
        let mut func = vec![];
        for function in &window.func {
            func.push(WindowFunctionInfo::try_create(function, &input_schema)?);
        }
        // Window
        self.main_pipeline.add_transform(|input, output| {
            // The transform can only be created here, because it cannot be cloned.

            let transform = if window.window_frame.units.is_rows() {
                let start_bound = FrameBound::try_from(&window.window_frame.start_bound)?;
                let end_bound = FrameBound::try_from(&window.window_frame.end_bound)?;
                Box::new(TransformWindow::<u64>::try_create_rows(
                    input,
                    output,
                    func.clone(),
                    partition_by.clone(),
                    order_by.clone(),
                    (start_bound, end_bound),
                )?) as Box<dyn Processor>
            } else {
                if order_by.len() == 1 {
                    // If the length of order_by is 1, there may be a RANGE frame.
                    let data_type = input_schema
                        .field(order_by[0].offset)
                        .data_type()
                        .remove_nullable();
                    with_number_mapped_type!(|NUM_TYPE| match data_type {
                        DataType::Number(NumberDataType::NUM_TYPE) => {
                            let start_bound =
                                FrameBound::try_from(&window.window_frame.start_bound)?;
                            let end_bound = FrameBound::try_from(&window.window_frame.end_bound)?;
                            return Ok(ProcessorPtr::create(Box::new(
                                TransformWindow::<NUM_TYPE>::try_create_range(
                                    input,
                                    output,
                                    func.clone(),
                                    partition_by.clone(),
                                    order_by.clone(),
                                    (start_bound, end_bound),
                                )?,
                            )
                                as Box<dyn Processor>));
                        }
                        _ => {}
                    })
                }

                // There is no offset in the RANGE frame. (just CURRENT ROW or UNBOUNDED)
                // So we can use any number type to create the transform.
                let start_bound = FrameBound::try_from(&window.window_frame.start_bound)?;
                let end_bound = FrameBound::try_from(&window.window_frame.end_bound)?;
                Box::new(TransformWindow::<u8>::try_create_range(
                    input,
                    output,
                    func.clone(),
                    partition_by.clone(),
                    order_by.clone(),
                    (start_bound, end_bound),
                )?) as Box<dyn Processor>
            };
            Ok(ProcessorPtr::create(transform))
        })?;
        if partition_by.is_empty() {
            self.main_pipeline.try_resize(old_output_len)?;
        }
        Ok(())
    }

    pub(crate) fn build_window_partition_pipeline(
        &mut self,
        window_partition: &WindowPartition,
    ) -> Result<()> {
        self.build_pipeline(&window_partition.input)?;

        let plan_schema = window_partition.output_schema()?;

        let partition_by = window_partition
            .partition_by
            .iter()
            .map(|index| plan_schema.index_of(&index.to_string()))
            .collect::<Result<Vec<_>>>()?;

        let sort_desc = window_partition
            .order_by
            .iter()
            .map(|desc| {
                let offset = plan_schema.index_of(&desc.order_by.to_string())?;
                Ok(SortColumnDescription {
                    offset,
                    asc: desc.asc,
                    nulls_first: desc.nulls_first,
                    is_nullable: plan_schema.field(offset).is_nullable(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                TransformWindowPartitionScatter::try_create(
                    self.ctx.clone(),
                    input,
                    output,
                    partition_by.clone(),
                )?,
            ))
        })?;

        let operator = DataOperator::instance().operator();
        let location_prefix =
            query_spill_prefix(self.ctx.get_tenant().tenant_name(), &self.ctx.get_id());
        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                TransformWindowPartitionSpillWriter::create(
                    self.ctx.clone(),
                    input,
                    output,
                    operator.clone(),
                    location_prefix.clone(),
                ),
            ))
        })?;

        let input_nums = self.main_pipeline.output_len();
        let transform = TransformWindowPartitionBucket::create(input_nums)?;

        let inputs = transform.get_inputs();
        let output = transform.get_output();

        self.main_pipeline
            .add_pipe(Pipe::create(inputs.len(), 1, vec![PipeItem::create(
                ProcessorPtr::create(Box::new(transform)),
                inputs,
                vec![output],
            )]));

        self.main_pipeline.try_resize(input_nums)?;

        self.main_pipeline.add_transform(|input, output| {
            TransformWindowPartitionSpillReader::create(input, output, operator.clone())
        })?;

        let block_size = self.settings.get_max_block_size()? as usize;
        let sort_spilling_batch_bytes = self.ctx.get_settings().get_sort_spilling_batch_bytes()?;
        let enable_loser_tree = self.ctx.get_settings().get_enable_loser_tree_merge_sort()?;
        let have_order_col = window_partition.after_exchange.unwrap_or(false);

        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                TransformWindowPartitionSort::try_create(
                    input,
                    output,
                    sort_desc.clone(),
                    plan_schema.clone(),
                    block_size,
                    sort_spilling_batch_bytes,
                    enable_loser_tree,
                    have_order_col,
                )?,
            ))
        })?;

        Ok(())
    }
}
