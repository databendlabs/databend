// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_functions::IFunction;
use common_planners::ExpressionPlan;
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;

// Executes certain expressions over the block.
// The expression consists of column identifiers from the block, constants, common functions.
// For example: hits * 2 + 3.
// ExpressionTransform normally used for transform internal, such as ProjectionTransform.
// Aims to transform a block to another format, such as add one column.
//
// Another example:
// SELECT (number+1) as c1, number as c2 from numbers_mt(10) ORDER BY c1,c2;
// Expression transform will make two fields on the base field: number:
// c1, c2
pub struct ExpressionTransform {
    funcs: Vec<Box<dyn IFunction>>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>
}

impl ExpressionTransform {
    pub fn try_create(schema: DataSchemaRef, exprs: Vec<ExpressionPlan>) -> Result<Self> {
        let mut fields = schema.fields().clone();
        let mut funcs = vec![];

        for expr in &exprs {
            let func = expr.to_function()?;
            if func.is_aggregator() {
                return Result::Err(ErrorCodes::BadTransformType(
                    format!(
                        "Aggregate function {} is found in ExpressionTransform, should AggregatorTransform",
                        func
                    )
                ));
            }
            funcs.push(func);

            // Merge fields.
            let field = expr.to_data_field(&schema)?;
            if !fields.iter().any(|x| x.name() == field.name()) {
                fields.push(field);
            }
        }

        let schema = DataSchemaRefExt::create(fields);
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

            let mut columns = Vec::with_capacity(funcs.len());
            for func in funcs {
                // Check if the column is already have in the input block.
                if let Some(col) = block.column_by_name(format!("{}", func).as_str()) {
                    columns.push(col.clone());
                } else {
                    columns.push(func.eval(&block)?.to_array(rows)?);
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
