// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datastreams::{ScalarExpressionStream, SendableDataBlockStream};
use crate::datavalues::{DataField, DataSchema, DataSchemaRef};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::Function;
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};

pub struct ProjectionTransform {
    funcs: Vec<Function>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
}

impl ProjectionTransform {
    pub fn try_create(exprs: Vec<ExpressionPlan>) -> FuseQueryResult<Self> {
        for expr in &exprs {
            if expr.has_aggregator() {
                return Err(FuseQueryError::Internal(format!(
                    "Unsupported aggregator function: {:?}",
                    expr
                )));
            }
        }

        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            funcs.push(expr.to_function()?);
        }

        Ok(ProjectionTransform {
            funcs,
            schema: Arc::new(DataSchema::empty()),
            input: Arc::new(EmptyProcessor::create()),
        })
    }

    pub fn expression_executor(
        schema: &DataSchemaRef,
        block: DataBlock,
        funcs: Vec<Function>,
    ) -> FuseQueryResult<DataBlock> {
        let mut arrays = Vec::with_capacity(funcs.len());
        for mut func in funcs.clone() {
            func.eval(&block)?;
            arrays.push(func.result()?.to_array(block.num_rows())?);
        }
        Ok(DataBlock::create(schema.clone(), arrays))
    }
}

#[async_trait]
impl IProcessor for ProjectionTransform {
    fn name(&self) -> &'static str {
        "ProjectionTransform"
    }

    fn schema(&self) -> FuseQueryResult<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        let input_schema = self.input.schema()?;

        let fields: FuseQueryResult<Vec<_>> = self
            .funcs
            .iter()
            .map(|func| {
                Ok(DataField::new(
                    format!("{:?}", func).as_str(),
                    func.return_type(&input_schema)?,
                    func.nullable(&input_schema)?,
                ))
            })
            .collect();
        self.schema = Arc::new(DataSchema::new(fields?));
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(ScalarExpressionStream::try_create(
            self.input.execute().await?,
            self.schema()?,
            self.funcs.clone(),
            ProjectionTransform::expression_executor,
        )?))
    }
}
