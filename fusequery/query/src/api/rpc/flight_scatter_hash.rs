// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;

use crate::api::rpc::flight_scatter::FlightScatter;
use crate::pipelines::transforms::ExpressionExecutor;

pub struct HashFlightScatter {
    scatter_expression_executor: Arc<ExpressionExecutor>,
    scatter_expression_name: String,
    scattered_size: usize,
}

impl FlightScatter for HashFlightScatter {
    fn try_create(
        schema: DataSchemaRef,
        expr: Option<Expression>,
        num: usize,
    ) -> common_exception::Result<Self> {
        match expr {
            None => Err(ErrorCode::LogicalError(
                "Hash flight scatter need expression.",
            )),
            Some(expr) => HashFlightScatter::try_create_impl(schema, num, expr),
        }
    }

    fn execute(&self, data_block: &DataBlock) -> common_exception::Result<Vec<DataBlock>> {
        let expression_executor = self.scatter_expression_executor.clone();
        let evaluated_data_block = expression_executor.execute(data_block)?;
        match evaluated_data_block.column_by_name(&self.scatter_expression_name) {
            None => common_exception::Result::Err(ErrorCode::LogicalError(
                "Logical error: expression executor error.",
            )),
            Some(indices) => DataBlock::scatter_block(data_block, indices, self.scattered_size),
        }
    }
}

impl HashFlightScatter {
    fn try_create_impl(schema: DataSchemaRef, num: usize, expr: Expression) -> Result<Self> {
        let expression = Self::expr_action(num, expr);
        let indices_expr_executor = Self::expr_executor(schema, &expression)?;
        indices_expr_executor.validate()?;

        Ok(HashFlightScatter {
            scatter_expression_executor: Arc::new(indices_expr_executor),
            scatter_expression_name: expression.column_name(),
            scattered_size: num,
        })
    }

    fn indices_expr_schema(output_name: &str) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![DataField::new(output_name, DataType::UInt64, false)])
    }

    fn expr_executor(schema: DataSchemaRef, expr: &Expression) -> Result<ExpressionExecutor> {
        ExpressionExecutor::try_create(
            "indices expression in FlightScatterByHash",
            schema,
            Self::indices_expr_schema(&expr.column_name()),
            vec![expr.clone()],
            false,
        )
    }

    fn expr_action(num: usize, expr: Expression) -> Expression {
        Expression::ScalarFunction {
            op: String::from("modulo"),
            args: vec![
                Expression::Cast {
                    expr: Box::new(expr),
                    data_type: DataType::UInt64,
                },
                Expression::Literal(DataValue::UInt64(Some(num as u64))),
            ],
        }
    }
}
