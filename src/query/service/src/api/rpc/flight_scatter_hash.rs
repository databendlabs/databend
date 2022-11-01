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
use common_functions::scalars::FunctionContext;
use common_sql::evaluator::EvalNode;
use common_sql::evaluator::Evaluator;
use common_sql::executor::PhysicalScalar;

use crate::api::rpc::flight_scatter::FlightScatter;
use crate::sessions::QueryContext;

#[derive(Clone)]
pub struct HashFlightScatter {
    scatter_expression_executor: Arc<EvalNode>,
    scattered_size: usize,
}

impl HashFlightScatter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        expr: Option<PhysicalScalar>,
        num: usize,
    ) -> Result<Self> {
        match expr {
            None => Err(ErrorCode::LogicalError(
                "Hash flight scatter need expression.",
            )),
            Some(expr) => HashFlightScatter::try_create_impl(schema, num, expr, ctx),
        }
    }
}

impl FlightScatter for HashFlightScatter {
    fn execute(&self, data_block: &DataBlock, _num: usize) -> Result<Vec<DataBlock>> {
        let expression_executor = self.scatter_expression_executor.clone();
        let indices = expression_executor
            .eval(&FunctionContext::default(), data_block)?
            .vector;

        let col: &PrimitiveColumn<u64> = Series::check_get(&indices)?;
        let indices: Vec<usize> = col.iter().map(|c| *c as usize).collect();
        DataBlock::scatter_block(data_block, &indices, self.scattered_size)
    }
}

impl HashFlightScatter {
    fn try_create_impl(
        _schema: DataSchemaRef,
        num: usize,
        expr: PhysicalScalar,
        _ctx: Arc<QueryContext>,
    ) -> Result<Self> {
        let expression = Self::expr_action(num, expr);
        let indices_expr_executor = Evaluator::eval_physical_scalar(&expression)?;

        Ok(HashFlightScatter {
            scatter_expression_executor: Arc::new(indices_expr_executor),
            scattered_size: num,
        })
    }

    fn expr_action(num: usize, expr: PhysicalScalar) -> PhysicalScalar {
        PhysicalScalar::Function {
            name: String::from("modulo"),
            args: vec![
                PhysicalScalar::Cast {
                    input: Box::new(expr),
                    target: u64::to_data_type(),
                },
                PhysicalScalar::Constant {
                    value: DataValue::UInt64(num as u64),
                    data_type: u64::to_data_type(),
                },
            ],
            return_type: u64::to_data_type(),
        }
    }
}
