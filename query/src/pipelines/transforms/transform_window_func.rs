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
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute::partition::lexicographical_partition_ranges;
use common_arrow::arrow::compute::sort::SortColumn;
use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_datavalues::ColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::Series;
use common_functions::aggregates::eval_aggr;
use common_functions::aggregates::AggregateFunctionFactory;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use enum_extract::let_extract;
use futures::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

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

    /// evaluate range frame
    /// requires that the block's already sorted by expressions of partition by and order by sub stmts
    async fn evaluate_window_func_col(
        &self,
        block: &DataBlock,
    ) -> common_exception::Result<ColumnRef> {
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

        match window_frame {
            None => {
                // at the moment, only supports aggr function
                let is_aggr_func = AggregateFunctionFactory::instance().check(&op);
                match is_aggr_func {
                    true => {
                        let mut arguments: Vec<DataField> = Vec::with_capacity(args.len());
                        for arg in args {
                            arguments.push(arg.to_data_field(&self.input_schema)?);
                        }

                        let mut sort_cols: Vec<SortColumn> =
                            Vec::with_capacity(partition_by.len() + order_by.len());
                        let partition_by_arrow_array = partition_by
                            .iter()
                            .map(|expr| {
                                block
                                    .try_column_by_name(&expr.column_name())
                                    .unwrap()
                                    .as_arrow_array()
                            })
                            .collect::<Vec<ArrayRef>>();
                        let partition_by_column = partition_by_arrow_array
                            .iter()
                            .map(|array| SortColumn {
                                values: array.as_ref(),
                                options: None,
                            })
                            .collect::<Vec<SortColumn>>();

                        sort_cols.extend(partition_by_column);
                        // todo process sort expr

                        let peer_rows = lexicographical_partition_ranges(&sort_cols).unwrap();

                        let window_cols = peer_rows
                            .map(|range| {
                                let offset = range.start;
                                let length = range.end - range.start;
                                let peered = block.slice(offset, length);
                                let args = arguments
                                    .iter()
                                    .map(|f| {
                                        let arg = peered.try_column_by_name(f.name()).unwrap();
                                        ColumnWithField::new(arg.clone(), f.to_owned())
                                    })
                                    .collect::<Vec<_>>();
                                let agg_result =
                                    eval_aggr(op, params.to_owned(), &args, peered.num_rows())
                                        .unwrap();
                                Series::concat(
                                    &(0..length).map(|_| agg_result.clone()).collect::<Vec<_>>(),
                                )
                                .unwrap()
                            })
                            .collect::<Vec<_>>();
                        Ok(Series::concat(&window_cols).unwrap())
                    }
                    false => unimplemented!(),
                }
            }
            Some(window_frame) => unimplemented!("not yet support window frame"),
        }
    }
}

#[async_trait::async_trait]
impl Processor for WindowFuncTransform {
    fn name(&self) -> &str {
        "WindowAggrFuncTransform"
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

    #[tracing::instrument(level = "debug", name = "window_aggr_func_execute", skip(self))]
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
        let window_col = self.evaluate_window_func_col(&block).await.unwrap();

        // add window func result column to the block
        let block = block
            .add_column(
                window_col,
                self.window_func.to_data_field(&self.input_schema).unwrap(),
            )
            .unwrap();

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
