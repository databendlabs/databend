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
use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

use bumpalo::Bump;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute::partition::lexicographical_partition_ranges;
use common_arrow::arrow::compute::sort::SortColumn;
use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_datavalues::ColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::Series;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::aggregates::AggregateFunctionRef;
use common_functions::aggregates::StateAddr;
use common_functions::scalars::assert_numeric;
use common_functions::window::WindowFrame;
use common_functions::window::WindowFrameBound;
use common_functions::window::WindowFrameUnits;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use enum_extract::let_extract;
use futures::StreamExt;
use segment_tree::ops::Commutative;
use segment_tree::ops::Identity;
use segment_tree::ops::Operation;
use segment_tree::SegmentPoint;

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

    /// evaluate window function for each frame and return the result block
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
        let mut partition_sort_exprs: Vec<Expression> =
            Vec::with_capacity(partition_by.len() + order_by.len());
        partition_sort_exprs.extend(
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
        partition_sort_exprs.extend(order_by.to_owned());

        let block = if !partition_sort_exprs.is_empty() {
            let sort_column_desc = get_sort_descriptions(block.schema(), &partition_sort_exprs)?;
            DataBlock::sort_block(block, &sort_column_desc, None)?
        } else {
            block.to_owned()
        };

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
        let mut arg_fields = Vec::with_capacity(args.len());
        for arg in args {
            let arg_field: DataField = arg.to_data_field(&self.input_schema)?;
            arg_fields.push(arg_field.clone());
            let arg_column = block.try_column_by_name(arg_field.name()).unwrap();
            arguments.push(ColumnWithField::new(Arc::clone(arg_column), arg_field));
        }

        let function = if !AggregateFunctionFactory::instance().check(op) {
            unimplemented!("not yet impl built-in window func");
        } else {
            AggregateFunctionFactory::instance().get(op, params.clone(), arg_fields)?
        };

        let arena = Arc::new(Bump::with_capacity(
            2 * block.num_rows() * function.state_layout().size(),
        ));
        let segment_tree_state = Self::new_partition_aggr_func(
            function.clone(),
            &arguments
                .iter()
                .map(|a| a.column().clone())
                .collect::<Vec<_>>(),
            arena,
        );

        let window_col_per_tuple = (0..block.num_rows())
            .map(|i| {
                let frame = &frame_bounds[i];
                let frame_start = frame.start;
                let frame_end = frame.end;
                let state = segment_tree_state.query(frame_start, frame_end);
                let mut builder = function.return_type().unwrap().create_mutable(1);
                function.merge_result(state, builder.as_mut()).unwrap();
                builder.to_column()
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

        let partition_boundaries = if !partition_by_sort_column.is_empty() {
            lexicographical_partition_ranges(&partition_by_sort_column)
                .unwrap()
                .collect::<Vec<_>>()
        } else {
            vec![0..block.num_rows(); 1]
        };
        let mut partition_boundaries = partition_boundaries.into_iter();
        let mut partition = partition_boundaries.next().unwrap();

        match (frame_units, start, end) {
            (_, WindowFrameBound::Preceding(None), WindowFrameBound::Following(None)) => (0..block
                .num_rows())
                .map(|i| {
                    if i >= partition.end && i < block.num_rows() {
                        partition = partition_boundaries.next().unwrap();
                    }
                    partition.clone()
                })
                .collect::<Vec<_>>(),
            (WindowFrameUnits::Rows, frame_start, frame_end) => (0..block.num_rows())
                .map(|i| {
                    if i >= partition.end && i < block.num_rows() {
                        partition = partition_boundaries.next().unwrap();
                    }
                    let mut start = partition.start;
                    let mut end = partition.end;
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
                        WindowFrameBound::Following(Some(following)) => {
                            start = std::cmp::min(end, i + following as usize)
                        }
                        _ => (),
                    }
                    match frame_end {
                        WindowFrameBound::Preceding(Some(preceding)) => {
                            end = std::cmp::max(start, i + 1 - preceding as usize);
                        }
                        WindowFrameBound::CurrentRow => {
                            end = i + 1;
                        }
                        WindowFrameBound::Following(Some(following)) => {
                            end = std::cmp::min(end, i + 1 + following as usize);
                        }
                        _ => (),
                    }
                    start..end
                })
                .collect::<Vec<_>>(),
            (WindowFrameUnits::Range, frame_start, frame_end) => match (frame_start, frame_end) {
                (WindowFrameBound::Preceding(None), WindowFrameBound::CurrentRow) => {
                    let mut partition_by_sort_column = partition_by_sort_column;
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

                    let mut peered_boundaries =
                        lexicographical_partition_ranges(&partition_by_sort_column).unwrap();
                    let mut peered = peered_boundaries.next().unwrap();
                    (0..block.num_rows())
                        .map(|i| {
                            if i >= partition.end && i < block.num_rows() {
                                partition = partition_boundaries.next().unwrap();
                            }
                            if i >= peered.end && i < block.num_rows() {
                                peered = peered_boundaries.next().unwrap();
                            }
                            partition.start..peered.end
                        })
                        .collect::<Vec<_>>()
                }
                (WindowFrameBound::CurrentRow, WindowFrameBound::Following(None)) => {
                    let mut partition_by_sort_column = partition_by_sort_column;
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

                    let mut peered_boundaries =
                        lexicographical_partition_ranges(&partition_by_sort_column).unwrap();
                    let mut peered = peered_boundaries.next().unwrap();
                    (0..block.num_rows())
                        .map(|i| {
                            if i >= partition.end && i < block.num_rows() {
                                partition = partition_boundaries.next().unwrap();
                            }
                            if i >= peered.end && i < block.num_rows() {
                                peered = peered_boundaries.next().unwrap();
                            }
                            peered.start..partition.end
                        })
                        .collect::<Vec<_>>()
                }
                (frame_start, frame_end) => {
                    assert_eq!(
                            order_by.len(),
                            1,
                            "Range mode is only possible if the query has exactly one numeric order by expression."
                        );
                    assert_numeric(
                            block
                                .schema()
                                .field_with_name(&order_by[0].column_name())
                                .unwrap()
                                .data_type(),
                        )
                            .expect(
                                "Range mode is only possible if the query has exactly one numeric order by expression.",
                            );

                    let order_by_values = block
                        .try_column_by_name(&order_by[0].column_name())
                        .unwrap()
                        .to_values();

                    (0..block.num_rows())
                        .map(|i| {
                            let current_value = &order_by_values[i];
                            if i >= partition.end && i < block.num_rows() {
                                partition = partition_boundaries.next().unwrap();
                            }
                            let mut start = partition.start;
                            let mut end = partition.end;
                            match frame_start {
                                WindowFrameBound::Preceding(Some(preceding)) => {
                                    let idx = order_by_values.partition_point(|x| {
                                        let min = DataValue::from(
                                            current_value.as_f64().unwrap() - preceding as f64,
                                        );
                                        x.cmp(&min) == Ordering::Less
                                    });
                                    start = std::cmp::max(start, idx);
                                }
                                WindowFrameBound::CurrentRow => {
                                    start = i;
                                }
                                WindowFrameBound::Following(Some(following)) => {
                                    let idx = order_by_values.partition_point(|x| {
                                        let max = DataValue::from(
                                            current_value.as_f64().unwrap() + following as f64,
                                        );
                                        x.cmp(&max) == Ordering::Less
                                    });
                                    start = std::cmp::min(end, idx);
                                }
                                _ => (),
                            }
                            match frame_end {
                                WindowFrameBound::Preceding(Some(preceding)) => {
                                    let idx = order_by_values.partition_point(|x| {
                                        let min = DataValue::from(
                                            current_value.as_f64().unwrap() - preceding as f64,
                                        );
                                        x.cmp(&min) != Ordering::Greater
                                    });
                                    end = std::cmp::max(start, idx);
                                }
                                WindowFrameBound::CurrentRow => {
                                    end = i + 1;
                                }
                                WindowFrameBound::Following(Some(following)) => {
                                    let idx = order_by_values.partition_point(|x| {
                                        let max = DataValue::from(
                                            current_value.as_f64().unwrap() + following as f64,
                                        );
                                        x.cmp(&max) != Ordering::Greater
                                    });
                                    end = std::cmp::min(end, idx);
                                }
                                _ => (),
                            }
                            start..end
                        })
                        .collect::<Vec<_>>()
                }
            },
        }
    }

    /// Actually, the computation is row based
    fn new_partition_aggr_func(
        func: AggregateFunctionRef,
        arguments: &[ColumnRef],
        arena: Arc<Bump>,
    ) -> SegmentPoint<StateAddr, Agg> {
        let rows = arguments[0].len();
        let state_per_tuple = (0..rows)
            .map(|i| {
                arguments
                    .iter()
                    .map(|c| c.slice(i, 1))
                    .collect::<Vec<ColumnRef>>()
            })
            .map(|args| {
                let place = arena.alloc_layout(func.state_layout());
                let state_addr = place.into();
                func.init_state(state_addr);
                func.accumulate(state_addr, &args, None, 1)
                    .expect("Failed to initialize the state");
                state_addr
            })
            .collect::<Vec<_>>();
        SegmentPoint::build(state_per_tuple, Agg { func, arena })
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

struct Agg {
    func: AggregateFunctionRef,
    arena: Arc<Bump>,
}

impl Operation<StateAddr> for Agg {
    fn combine(&self, a: &StateAddr, b: &StateAddr) -> StateAddr {
        let place = self.arena.alloc_layout(self.func.state_layout());
        let state_addr = place.into();
        self.func.init_state(state_addr);
        self.func
            .merge(state_addr, *a)
            .expect("Failed to merge states");
        self.func
            .merge(state_addr, *b)
            .expect("Failed to merge states");
        state_addr
    }

    fn combine_mut(&self, a: &mut StateAddr, b: &StateAddr) {
        self.func.merge(*a, *b).expect("Failed to merge states");
    }

    fn combine_mut2(&self, a: &StateAddr, b: &mut StateAddr) {
        self.func.merge(*b, *a).expect("Failed to merge states");
    }
}

impl Commutative<StateAddr> for Agg {}

impl Identity<StateAddr> for Agg {
    fn identity(&self) -> StateAddr {
        let place = self.arena.alloc_layout(self.func.state_layout());
        let state_addr = place.into();
        self.func.init_state(state_addr);
        state_addr
    }
}
