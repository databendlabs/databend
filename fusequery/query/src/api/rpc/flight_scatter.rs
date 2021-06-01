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
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::Expression;

use crate::pipelines::transforms::ExpressionExecutor;

pub struct FlightScatterByHash {
    scatter_expression_executor: Arc<ExpressionExecutor>,
    scatter_expression_name: String,
    scattered_size: usize
}

impl FlightScatterByHash {
    pub fn try_create(
        schema: DataSchemaRef,
        action: Expression,
        num: usize
    ) -> Result<FlightScatterByHash> {
        let indices_expression_action = Expression::ScalarFunction {
            op: String::from("modulo"),
            args: vec![
                Expression::Cast {
                    expr: Box::new(action),
                    data_type: DataType::UInt64
                },
                Expression::Literal(DataValue::UInt64(Some(num as u64))),
            ]
        };

        let output_name = indices_expression_action.column_name();
        let expression_executor = ExpressionExecutor::try_create(
            schema,
            DataSchemaRefExt::create(vec![DataField::new(&output_name, DataType::UInt64, false)]),
            vec![indices_expression_action],
            false
        )?;
        expression_executor.validate()?;

        Ok(FlightScatterByHash {
            scatter_expression_executor: Arc::new(expression_executor),
            scatter_expression_name: output_name,
            scattered_size: num
        })
    }

    pub fn execute(&self, data_block: &DataBlock) -> Result<Vec<DataBlock>> {
        let expression_executor = self.scatter_expression_executor.clone();
        match expression_executor
            .execute(data_block)?
            .column_by_name(&self.scatter_expression_name)
        {
            None => Result::Err(ErrorCodes::LogicalError(
                "Logical error: expression executor error."
            )),
            Some(indices) => DataBlock::scatter_block(data_block, indices, self.scattered_size)
        }
    }
}
