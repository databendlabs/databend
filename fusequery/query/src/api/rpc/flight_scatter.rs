// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_planners::ExpressionAction;
use common_datablocks::DataBlock;
use common_exception::{Result, ErrorCodes};
use common_functions::{IFunction, CastFunction, ArithmeticModuloFunction, ColumnFunction, LiteralFunction};
use common_datavalues::{DataSchema, DataSchemaRef, DataType, DataValue, DataSchemaRefExt, DataField};
use crate::pipelines::transforms::ExpressionExecutor;
use std::sync::Arc;

pub struct FlightScatter(Arc<ExpressionExecutor>, String, usize);

impl FlightScatter {
    pub fn try_create(schema: DataSchemaRef, action: ExpressionAction, num: usize) -> Result<FlightScatter> {
        let indices_expression_action = ExpressionAction::ScalarFunction {
            op: String::from("modulo"),
            args: vec![
                ExpressionAction::Cast {
                    expr: Box::new(action),
                    data_type: DataType::UInt64,
                },
                ExpressionAction::Literal(DataValue::UInt64(Some(num as u64)))
            ],
        };

        let output_name = indices_expression_action.column_name();
        let expression_executor = ExpressionExecutor::try_create(
            schema, DataSchemaRefExt::create(vec![
                DataField::new(
                    &output_name,
                    DataType::UInt64,
                    false,
                )
            ]),
            vec![indices_expression_action], false,
        )?;
        expression_executor.validate()?;


        Ok(FlightScatter(Arc::new(expression_executor), output_name, num))
    }

    pub fn execute(&self, data_block: &DataBlock) -> Result<Vec<DataBlock>> {
        let expression_executor = self.0.clone();
        match expression_executor.execute(data_block)?.column_by_name(&self.1) {
            None => Result::Err(ErrorCodes::LogicalError("Logical error: expression executor error.")),
            Some(indices) => DataBlock::scatter_block(data_block, &indices.to_array()?, self.2),
        }
    }
}
