// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::{DataSchemaRef, DataArrayRef, DataColumnarValue};
use common_exception::ErrorCodes;
use common_exception::Result;
use common_functions::{IFunction, FunctionFactory, AliasFunction, ColumnFunction, LiteralFunction, CastFunction};
use common_planners::ExpressionAction;
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;
use crate::sessions::FuseQueryContextRef;
use std::collections::HashMap;
use common_aggregate_functions::AggregateFunctionFactory;
use common_arrow::arrow::datatypes::DataType;

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
    // The final schema(Build by plan_builder.expression).
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
    executor: Arc<ExpressionExecutor>,
}

impl ExpressionTransform {
    pub fn try_create(schema: DataSchemaRef, exprs: Vec<ExpressionAction>) -> Result<Self> {
        let mut executor = ExpressionExecutor::create(schema.clone(), exprs);
        executor.prepare_expr_chain()?;
        executor.validate()?;

        Ok(ExpressionTransform {
            schema: schema.clone(),
            input: Arc::new(EmptyProcessor::create()),
            executor: Arc::new(executor),
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
        let stream = self.input.filter_map(move |v| {
            executor.execute(v)
        });

        Ok(Box::pin(stream))
    }
}

struct ExpressionFunction {
    name: String,
    func: Box<dyn IFunction>,
    arg_names: Vec<String>,
    arg_types: Vec<DataType>,
    return_type: DataType,
}

#[derive(Clone, Debug)]
struct ExpressionExecutor {
    schema: DataSchemaRef,
    exprs: Vec<ExpressionAction>,
    chain: Vec<ExpressionFunction>,
}


impl ExpressionExecutor {
    pub fn create(schema: DataSchemaRef, exprs: Vec<ExpressionAction>) -> Self {
        Self {
            schema,
            exprs,
            chain: vec![],
        }
    }

    pub fn prepare_expr_chain(&mut self) -> Result<()> {
        self.exprs.iter().for_each(|expr| self.build_chain(expr)?);
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        Ok(())
    }

    fn build_chain(&mut self, expr: &ExpressionAction) -> Result<()> {
        match expr {
            ExpressionAction::Alias(name, sub_expr) => {
                self.build_chain(expr)?;
                let func = AliasFunction::try_create(name.clone())?;
                let arg_types = vec![expr.to_data_type(&self.schema)?];
                let return_type = func.return_type(&arg_types)?;
                self.chain.push(ExpressionFunction {
                    name: expr.column_name(),
                    func,
                    arg_names: vec![sub_expr.column_name()],
                    arg_types,
                    return_type,
                });
            }
            ExpressionAction::Column(c) => {
                let func = ColumnFunction::try_create(c)?;
                let arg_type = self.schema.field_with_name(c).map_err(ErrorCodes::from_arrow)?.data_type();
                self.chain.push(ExpressionFunction {
                    name: self.column_name(),
                    func,
                    arg_names: vec![c.to_string()],
                    arg_types: vec![arg_type.clone()],
                    return_type: arg_type.clone()?,
                });
            }
            ExpressionAction::Literal(l) => {
                let func = LiteralFunction::try_create(l.clone())?;
                self.chain.push(ExpressionFunction {
                    name: self.column_name(),
                    func,
                    arg_names: vec![],
                    arg_types: vec![],
                    return_type: l.data_type(),
                });
            }
            ExpressionAction::ScalarFunction { op, args } => {
                args.iter().for_each(|expr| self.build_chain(expr)?);

                let func = FunctionFactory::get(op)?;
                let arg_types = args.iter().map(|action| action.to_data_type(&self.schema)).collect::<Result<Vec<DataType>>>()?;
                let return_type = func.return_type(&arg_types)?;
                self.chain.push(ExpressionFunction {
                    name: self.column_name(),
                    func,
                    arg_names: args.iter().map(|action| action.column_name()).collect(),
                    arg_types,
                    return_type,
                });
            }
            ExpressionAction::AggregateFunction { op, args } => {
                args.iter().for_each(|expr| self.build_chain(expr)?);

                let func = FunctionFactory::get(op)?;
                let arg_types = args.iter().map(|action| action.to_data_type(&self.schema)).collect::<Result<Vec<DataType>>>()?;
                let return_type = func.return_type(&arg_types)?;
                self.chain.push(ExpressionFunction {
                    name: self.column_name(),
                    func,
                    arg_names: args.iter().map(|action| action.column_name()).collect(),
                    arg_types,
                    return_type,
                });
            }
            ExpressionAction::Sort { expr, asc, nulls_first } => {
                self.build_chain(expr);
            }

            ExpressionAction::Wildcard => {}
            ExpressionAction::Cast { expr: sub_expr, data_type } => {
                self.build_chain(expr)?;
                let func = CastFunction::create(data_type.clone());
                self.chain.push(ExpressionFunction {
                    name: self.column_name(),
                    func,
                    arg_names: vec![sub_expr.column_name().into()],
                    arg_types: vec![sub_expr.to_data_type(&self.schema)?],
                    return_type: data_type.clone(),
                });
            }
        }
        Ok(())
    }


    pub fn execute(&self, block: &DataBlock) -> Result<DataBlock> {
        let mut columns = vec![];
        let mut column_map = HashMap::new();
        let rows = block.num_rows();

        self.chain.iter().for_each(|action| {
            let column_name = action.name.clone();
            if !column_map.contains_key(&column_name) {
                let arg_columns
                    = action.arg_names.iter().map(|arg| column_map.get(arg).ok_or_else("error "))
                    .collect::<Vec<Result<DataColumnarValue>>>()?;

                let column = action.func.eval(&arg_columns, rows)?;
                column_map.insert(column_name.clone(), column.clone());
                columns.push(column);
            }
        });

        let mut project_columns = Vec::with_capacity(self.exprs.len());
        self.exprs.iter().for_each(|expr| {
            project_columns.push(column_map.get(&expr.column_name()).ok_or_else("error ")?.to_array()?);
        });

        // projection to remove unused columns
        Ok(DataBlock::create(self.schema.clone(), project_columns))
    }
}