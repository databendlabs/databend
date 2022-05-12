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

use common_datablocks::DataBlock;
use common_datavalues::BooleanType;
use common_datavalues::ColumnRef;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataTypeImpl;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::aggregates::AggregateFunctionRef;
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
        let mut data_blocks: Vec<DataBlock> = vec![];
        while let Some(block) = stream.next().await {
            let block = block?;
            data_blocks.push(block);
        }

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

        let mut arguments = Vec::with_capacity(args.len());
        for arg in args {
            arguments.push(arg.to_data_field(&self.input_schema)?);
        }

        let is_aggr_func = AggregateFunctionFactory::instance().check(&op);

        let window_cols = match &window_frame {
            Some(window_frame) if is_aggr_func => match &window_frame.units {
                WindowFrameUnits::Range => {
                    evaluate_range_with_aggr_func(
                        AggregateFunctionFactory::instance().get(
                            op,
                            params.to_owned(),
                            arguments,
                        )?,
                        &data_blocks,
                        window_frame.start_bound,
                        window_frame.end_bound,
                    )
                    .await?
                }
                WindowFrameUnits::Rows => {
                    evaluate_rows_with_aggr_func(
                        AggregateFunctionFactory::instance().get(
                            op,
                            params.to_owned(),
                            arguments,
                        )?,
                        &data_blocks,
                        window_frame.start_bound,
                        window_frame.end_bound,
                    )
                    .await?
                }
            },
            None if is_aggr_func => {
                evaluate_range_with_aggr_func(
                    AggregateFunctionFactory::instance().get(op, params.to_owned(), arguments)?,
                    &data_blocks,
                    WindowFrameBound::Preceding(None),
                    WindowFrameBound::CurrentRow,
                )
                .await?
            }
            _ => unimplemented!(),
        };

        // add window func result column to blocks
        let data_blocks = data_blocks
            .into_iter()
            .zip(window_cols)
            .map(|(block, window_col)| {
                block
                    .add_column(
                        window_col,
                        DataField::new("test", DataTypeImpl::Boolean(BooleanType {})),
                    )
                    .unwrap()
            })
            .collect();

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            data_blocks,
        )))
    }
}

/// evaluate range frame
async fn evaluate_range_with_aggr_func(
    func: AggregateFunctionRef,
    blocks: &[DataBlock],
    start_bound: WindowFrameBound,
    end_bound: WindowFrameBound,
) -> common_exception::Result<Vec<ColumnRef>> {
    let window_cols = Vec::with_capacity(blocks.len());
}

/// evaluate rows frame
async fn evaluate_rows_with_aggr_func(
    func: AggregateFunctionRef,
    blocks: &[DataBlock],
    start_bound: WindowFrameBound,
    end_bound: WindowFrameBound,
) -> common_exception::Result<Vec<ColumnRef>> {
    todo!()
}
