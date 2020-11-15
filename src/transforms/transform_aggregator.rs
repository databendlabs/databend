// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use async_std::stream::StreamExt;
use async_trait::async_trait;

use crate::datablocks::DataBlock;
use crate::datastreams::{ChunkStream, DataBlockStream};
use crate::datavalues::{DataField, DataSchema, DataType};
use crate::error::{Error, Result};
use crate::functions::{AggregateFunctionFactory, Function};
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};

pub struct CountTransform {
    name: &'static str,
    expr: Arc<ExpressionPlan>,
    column: Arc<Function>,
    data_type: DataType,
    input: Arc<dyn IProcessor>,
}

pub struct SumTransform {
    name: &'static str,
    expr: Arc<ExpressionPlan>,
    column: Arc<Function>,
    data_type: DataType,
    input: Arc<dyn IProcessor>,
}

pub struct MaxTransform {
    name: &'static str,
    expr: Arc<ExpressionPlan>,
    column: Arc<Function>,
    data_type: DataType,
    input: Arc<dyn IProcessor>,
}

pub enum AggregatorTransform {
    Count(CountTransform),
    Sum(SumTransform),
    Max(MaxTransform),
}

impl AggregatorTransform {
    pub fn create(
        name: &str,
        expr: Arc<ExpressionPlan>,
        column: Arc<Function>,
        data_type: &DataType,
    ) -> Result<AggregatorTransform> {
        Ok(match name.to_lowercase().as_str() {
            "count" => AggregatorTransform::Count(CountTransform {
                name: "CountTransform",
                expr,
                column,
                data_type: DataType::UInt64,
                input: Arc::new(EmptyProcessor::create()),
            }),
            "sum" => AggregatorTransform::Sum(SumTransform {
                name: "SumTransform",
                expr,
                column,
                data_type: data_type.clone(),
                input: Arc::new(EmptyProcessor::create()),
            }),
            "max" => AggregatorTransform::Max(MaxTransform {
                name: "MaxTransform",
                expr,
                column,
                data_type: data_type.clone(),
                input: Arc::new(EmptyProcessor::create()),
            }),
            _ => {
                return Err(Error::Unsupported(format!(
                    "Unsupported aggregators transform: {:?}",
                    name
                )))
            }
        })
    }
}

#[async_trait]
impl IProcessor for AggregatorTransform {
    fn name(&self) -> &'static str {
        match self {
            AggregatorTransform::Count(v) => v.name,
            AggregatorTransform::Max(v) => v.name,
            AggregatorTransform::Sum(v) => v.name,
        }
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) {
        match self {
            AggregatorTransform::Count(v) => v.input = input,
            AggregatorTransform::Max(v) => v.input = input,
            AggregatorTransform::Sum(v) => v.input = input,
        }
    }

    async fn execute(&self) -> Result<DataBlockStream> {
        let (expr, mut func, mut exec) = match self {
            AggregatorTransform::Count(v) => (
                v.expr.clone(),
                AggregateFunctionFactory::get("count", v.column.clone(), &v.data_type)?,
                v.input.execute().await?,
            ),
            AggregatorTransform::Sum(v) => (
                v.expr.clone(),
                AggregateFunctionFactory::get("sum", v.column.clone(), &v.data_type)?,
                v.input.execute().await?,
            ),
            AggregatorTransform::Max(v) => (
                v.expr.clone(),
                AggregateFunctionFactory::get("max", v.column.clone(), &v.data_type)?,
                v.input.execute().await?,
            ),
        };

        while let Some(v) = exec.next().await {
            func.accumulate(&v?)?;
        }

        Ok(Box::pin(ChunkStream::create(vec![DataBlock::new(
            DataSchema::new(vec![DataField::new(
                format!("{:?}", expr).as_str(),
                func.return_type(&DataSchema::empty())?,
                false,
            )]),
            vec![func.aggregate()?],
        )])))
    }
}
