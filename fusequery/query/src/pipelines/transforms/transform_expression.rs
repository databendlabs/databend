// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_aggregate_functions::AggregateFunctionFactory;
use common_arrow::arrow::datatypes::DataType;
use common_datablocks::DataBlock;
use common_datavalues::DataArrayRef;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_functions::AliasFunction;
use common_functions::CastFunction;
use common_functions::ColumnFunction;
use common_functions::FunctionFactory;
use common_functions::IFunction;
use common_functions::LiteralFunction;
use common_planners::ExpressionAction;
use common_planners::ExpressionChain;
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::FuseQueryContextRef;

/// Executes certain expressions over the block and append the result column to the new block.
/// Aims to transform a block to another format, such as add one or more columns against the Expressions.
///
/// Example:
/// SELECT (number+1) as c1, number as c2 from numbers_mt(10) ORDER BY c1,c2;
/// Expression transform will make two fields on the base field number:
/// Input block columns:
/// |number|
///
/// Append two columns:
/// |c1|c2|
///
/// So the final block:
/// |number|c1|c2|
pub struct ExpressionTransform {
    input_schema: DataSchemaRef,
    // The final schema(Build by plan_builder.expression).
    output_schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
    executor: Arc<ExpressionExecutor>
}

impl ExpressionTransform {
    pub fn try_create(
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        exprs: Vec<ExpressionAction>
    ) -> Result<Self> {
        let mut executor = ExpressionExecutor::try_create(input_schema.clone(), exprs, false)?;
        executor.validate()?;

        Ok(ExpressionTransform {
            input_schema,
            output_schema,
            input: Arc::new(EmptyProcessor::create()),
            executor: Arc::new(executor)
        })
    }
}

#[async_trait::async_trait]
impl IProcessor for ExpressionTransform {
    fn name(&self) -> &str {
        "ExpressionTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let executor = self.executor.clone();

        let execute_fn =
            |executor: Arc<ExpressionExecutor>, block: Result<DataBlock>| -> Result<DataBlock> {
                let block = block?;
                executor.execute(&block)
            };

        let stream = self.input.filter_map(move |v| {
            let executor = executor.clone();
            execute_fn(executor, v)
        });

        Ok(Box::pin(stream))
    }
}
