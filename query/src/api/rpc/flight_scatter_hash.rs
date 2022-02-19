// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
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
        let indices = evaluated_data_block.try_column_by_name(&self.scatter_expression_name)?;

        let col: &PrimitiveColumn<u64> = Series::check_get(indices)?;
        let indices: Vec<usize> = col.iter().map(|c| *c as usize).collect();
        DataBlock::scatter_block(data_block, &indices, self.scattered_size)
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
        DataSchemaRefExt::create(vec![DataField::new(output_name, u64::to_data_type())])
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
                    data_type: u64::to_data_type(),
                    is_nullable: false,
                },
                Expression::create_literal_with_type(
                    DataValue::UInt64(num as u64),
                    u64::to_data_type(),
                ),
            ],
        }
    }
}
