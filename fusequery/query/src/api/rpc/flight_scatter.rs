// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_planners::ExpressionAction;
use common_datablocks::DataBlock;
use common_exception::{Result, ErrorCodes};
use common_functions::{IFunction, CastFunction, ArithmeticModuloFunction, ColumnFunction, LiteralFunction};
use common_datavalues::{DataSchema, DataSchemaRef, DataType, DataValue};

pub struct FlightScatter(Box<dyn IFunction>, usize);

impl FlightScatter {
    pub fn try_create(action: ExpressionAction, num: usize) -> Result<FlightScatter> {
        if action.has_aggregator()? {
            return Result::Err(ErrorCodes::BadScatterExpression("Scatter expression cannot contain aggregate functions."));
        }

        Ok(FlightScatter(ArithmeticModuloFunction::try_create_func(
            "scatters",
            &vec![
                CastFunction::create(action.to_function()?, DataType::UInt64),
                LiteralFunction::try_create(DataValue::UInt64(Some(num as u64)))?,
            ],
        )?, num))
    }

    pub fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let rows = data_block.num_rows();
        let indices = self.0.eval(&data_block)?.to_array(rows)?;
        DataBlock::scatter_block(&data_block, &indices, self.1)
    }
}
