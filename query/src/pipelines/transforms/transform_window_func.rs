// Copyright 2022 Datafuse Labs.
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

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute::partition::lexicographical_partition_ranges;
use common_arrow::arrow::compute::sort::SortColumn;
use common_datablocks::DataBlock;
use common_datavalues::ColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::Series;
use common_functions::aggregates::eval_aggr;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::window::WindowFrame;
use common_functions::window::WindowFrameBound;
use common_functions::window::WindowFrameUnits;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use enum_extract::let_extract;
use futures::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::get_sort_descriptions;

pub struct WindowFuncTransform {
    window_func: Expression,
    schema: DataSchemaRef,
    input: Arc<dyn Processor>,
    input_schema: DataSchemaRef,
}

impl WindowFuncTransform {
    pub fn create(
        window_func: Expression,
        schema: DataSchemaRef,
        input_schema: DataSchemaRef,
    ) -> Self {
        WindowFuncTransform {
            window_func,
            schema,
            input: Arc::new(EmptyProcessor::create()),
            input_schema,
        }
    }

    /// evaluate window function for each frame and return the result column
    async fn evaluate_window_func(&self, block: &DataBlock) -> common_exception::Result<DataBlock> {
        // extract the window function
        let_extract!(
            Expression::WindowFunction {
                op,
                params,
                args,
                partition_by,
                order_by,
                window_frame
            },
            &self.window_func,
            panic!()
        );

        // sort block by partition_by and order_by exprs
        let mut sort_exprs: Vec<Expression> =
            Vec::with_capacity(partition_by.len() + order_by.len());
        sort_exprs.extend(
            partition_by
                .iter()
                .map(|part_by_expr| Expression::Sort {
                    expr: Box::new(part_by_expr.to_owned()),
                    asc: true,
                    nulls_first: false,
                    origin_expr: Box::new(part_by_expr.to_owned()),
                })
                .collect::<Vec<_>>(),
        );
        sort_exprs.extend(order_by.to_owned());
        let sort_column_desc = get_sort_descriptions(block.schema(), &sort_exprs)?;
        let block = DataBlock::sort_block(&block, &sort_column_desc, None)?;

        // set default window frame
        let window_frame = match window_frame {
            None => {
                // compute the whole partition
                match order_by.is_empty() {
                    // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    false => WindowFrame::default(),
                    // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    true => WindowFrame {
                        units: WindowFrameUnits::Range,
                        start_bound: WindowFrameBound::Preceding(None),
                        end_bound: WindowFrameBound::Following(None),
                    },
                }
            }
            Some(window_frame) => window_frame.to_owned(),
        };

        // determine window frame bounds of each tuple
        let frame_bounds = Self::frame_bounds_per_tuple(
            &block,
            partition_by,
            order_by,
            window_frame.units,
            window_frame.start_bound,
            window_frame.end_bound,
        );

        // function calculate
        let mut arguments: Vec<ColumnWithField> = Vec::with_capacity(args.len());
        for arg in args {
            let arg_field: DataField = arg.to_data_field(&self.input_schema)?;
            let arg_column = block.try_column_by_name(arg_field.name()).unwrap();
            arguments.push(ColumnWithField::new(Arc::clone(arg_column), arg_field));
        }

        let window_col_per_tuple = (0..block.num_rows())
            .map(|i| {
                let frame = &frame_bounds[i];
                let frame_start = frame.start;
                let frame_end = frame.end;
                let frame_size = frame_end - frame_start;
                let args_slice = arguments
                    .iter()
                    .map(|c| c.slice(frame_start, frame_size))
                    .collect::<Vec<_>>();
                // at the moment, only supports aggr function
                if !AggregateFunctionFactory::instance().check(op) {
                    unimplemented!("not yet impl built-in window func");
                }
                eval_aggr(op, params.to_owned(), &args_slice, frame_size).unwrap()
            })
            .collect::<Vec<_>>();

        let window_col = Series::concat(&window_col_per_tuple).unwrap();

        block.add_column(
            window_col,
            self.window_func.to_data_field(&self.input_schema).unwrap(),
        )
    }

    /// compute frame range for each tuple
    fn frame_bounds_per_tuple(
        block: &DataBlock,
        partition_by: &[Expression],
        order_by: &[Expression],
        frame_units: WindowFrameUnits,
        start: WindowFrameBound,
        end: WindowFrameBound,
    ) -> Vec<Range<usize>> {
        match (frame_units, start, end) {
            (_, WindowFrameBound::Preceding(None), WindowFrameBound::Following(None)) => {
                let partition_by_arrow_array = partition_by
                    .iter()
                    .map(|expr| {
                        block
                            .try_column_by_name(&expr.column_name())
                            .unwrap()
                            .as_arrow_array()
                    })
                    .collect::<Vec<ArrayRef>>();
                let partition_by_sort_column = partition_by_arrow_array
                    .iter()
                    .map(|array| SortColumn {
                        values: array.as_ref(),
                        options: None,
                    })
                    .collect::<Vec<SortColumn>>();
                let mut partition_boundaries =
                    lexicographical_partition_ranges(&partition_by_sort_column).unwrap();
                let mut partition = partition_boundaries.next().unwrap();
                (0..block.num_rows())
                    .map(|i| {
                        if i >= partition.end && i < block.num_rows() {
                            partition = partition_boundaries.next().unwrap();
                        }
                        partition.clone()
                    })
                    .collect::<Vec<_>>()
            }
            (frame_unit, frame_start, frame_end) => {
                let partition_by_arrow_array = partition_by
                    .iter()
                    .map(|expr| {
                        block
                            .try_column_by_name(&expr.column_name())
                            .unwrap()
                            .as_arrow_array()
                    })
                    .collect::<Vec<ArrayRef>>();
                let mut partition_by_sort_column = partition_by_arrow_array
                    .iter()
                    .map(|array| SortColumn {
                        values: array.as_ref(),
                        options: None,
                    })
                    .collect::<Vec<SortColumn>>();

                let order_by_arrow_array = order_by
                    .iter()
                    .map(|expr| {
                        block
                            .try_column_by_name(&expr.column_name())
                            .unwrap()
                            .as_arrow_array()
                    })
                    .collect::<Vec<ArrayRef>>();
                let order_by_sort_column = order_by_arrow_array
                    .iter()
                    .map(|array| SortColumn {
                        values: array.as_ref(),
                        options: None,
                    })
                    .collect::<Vec<SortColumn>>();
                partition_by_sort_column.extend(order_by_sort_column);

                let mut frame_boundaries =
                    lexicographical_partition_ranges(&partition_by_sort_column).unwrap();

                match frame_unit {
                    WindowFrameUnits::Rows => {
                        let mut frame_bound = frame_boundaries.next().unwrap();

                        (0..block.num_rows())
                            .map(|i| {
                                if i >= frame_bound.end && i < block.num_rows() {
                                    frame_bound = frame_boundaries.next().unwrap();
                                }
                                let mut start = frame_bound.start;
                                let mut end = frame_bound.end;
                                match frame_start {
                                    WindowFrameBound::Preceding(Some(preceding)) => {
                                        start = std::cmp::max(
                                            start,
                                            if i < preceding as usize {
                                                0
                                            } else {
                                                i - preceding as usize
                                            },
                                        );
                                    }
                                    WindowFrameBound::CurrentRow => {
                                        start = i;
                                    }
                                    _ => (),
                                }
                                match frame_end {
                                    WindowFrameBound::CurrentRow => {
                                        end = i;
                                    }
                                    WindowFrameBound::Following(Some(following)) => {
                                        end = std::cmp::min(end, i + 1 + following as usize);
                                    }
                                    _ => (),
                                }
                                start..end
                            })
                            .collect::<Vec<_>>()
                    }
                    WindowFrameUnits::Range => {
                        todo!()
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Processor for WindowFuncTransform {
    fn name(&self) -> &str {
        "WindowFuncTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> common_exception::Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    #[tracing::instrument(level = "debug", name = "window_func_execute", skip(self))]
    async fn execute(&self) -> common_exception::Result<SendableDataBlockStream> {
        let mut stream: SendableDataBlockStream = self.input.execute().await?;
        let mut blocks: Vec<DataBlock> = vec![];
        while let Some(block) = stream.next().await {
            let block = block?;
            blocks.push(block);
        }

        if blocks.is_empty() {
            return Ok(Box::pin(DataBlockStream::create(
                self.schema.clone(),
                None,
                vec![],
            )));
        }

        // combine blocks
        let schema = blocks[0].schema();

        let combined_columns = (0..schema.num_fields())
            .map(|i| {
                blocks
                    .iter()
                    .map(|block| block.column(i).clone())
                    .collect::<Vec<_>>()
            })
            .map(|columns| Series::concat(&columns).unwrap())
            .collect::<Vec<_>>();

        let block = DataBlock::create(schema.clone(), combined_columns);

        // evaluate the window function column
        let block = self.evaluate_window_func(&block).await.unwrap();

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
