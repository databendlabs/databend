// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_functions::IFunction;
use common_planners::ExpressionAction;
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;

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
    // Against Functions.
    funcs: Vec<Box<dyn IFunction>>,
    // The final schema(Build by plan_builder.expression).
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>
}

impl ExpressionTransform {
    pub fn try_create(schema: DataSchemaRef, exprs: Vec<ExpressionAction>, skip_aggregate: bool) -> Result<Self> {
        let mut funcs = vec![];

        for expr in &exprs {
            let func = expr.to_function()?;
            if func.has_aggregator() {
                return Result::Err(ErrorCodes::BadTransformType(
                    format!(
                        "Aggregate function {} is found in ExpressionTransform, should AggregatorTransform",
                        func
                    )
                ));
            }
            funcs.push(func);
        }

        Ok(ExpressionTransform {
            funcs,
            schema,
            input: Arc::new(EmptyProcessor::create())
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
        let funcs = self.funcs.clone();
        let schema = self.schema.clone();
        let input_stream = self.input.execute().await?;

        let executor = |schema: DataSchemaRef,
                        funcs: &[Box<dyn IFunction>],
                        block: Result<DataBlock>|
         -> Result<DataBlock> {
            let block = block?;
            let rows = block.num_rows();

            let mut columns = Vec::from(block.columns());
            for func in funcs {
                match block.column_by_name(format!("{}", func).as_str()) {
                    None => {
                        columns.push(func.eval(&block)?.to_array(rows)?);
                    }
                    Some(_) => {}
                }
            }
            Ok(DataBlock::create(schema, columns))
        };

        let stream = input_stream.filter_map(move |v| {
            executor(schema.clone(), funcs.as_slice(), v)
                .map(Some)
                .transpose()
        });
        Ok(Box::pin(stream))
    }
}
