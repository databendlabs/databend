// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use async_std::stream::StreamExt;
use async_trait::async_trait;

use crate::datablocks::DataBlock;
use crate::datastreams::{ChunkStream, DataBlockStream};
use crate::datavalues::{DataField, DataSchema, DataType};
use crate::error::Result;
use crate::functions::{AggregateFunctionFactory, Function};
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};

pub struct CountTransform {
    expr: Arc<ExpressionPlan>,
    column: Arc<Function>,
    data_type: DataType,
    input: Arc<dyn IProcessor>,
}

impl CountTransform {
    pub fn create(expr: Arc<ExpressionPlan>, column: Arc<Function>, data_type: &DataType) -> Self {
        CountTransform {
            expr,
            column,
            data_type: data_type.clone(),
            input: Arc::new(EmptyProcessor::create()),
        }
    }
}

#[async_trait]
impl IProcessor for CountTransform {
    fn name(&self) -> &'static str {
        "CountTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) {
        self.input = input;
    }

    async fn execute(&self) -> Result<DataBlockStream> {
        let mut func =
            AggregateFunctionFactory::get("count", self.column.clone(), &self.data_type)?;

        let mut exec = self.input.execute().await?;
        while let Some(v) = exec.next().await {
            func.accumulate(&v?)?;
        }

        Ok(Box::pin(ChunkStream::create(vec![DataBlock::new(
            DataSchema::new(vec![DataField::new(
                format!("{:?}", self.expr).as_str(),
                DataType::UInt64,
                false,
            )]),
            vec![func.aggregate()?],
        )])))
    }
}
