// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionRef;
use common_planners::Expression;

pub struct AggregatorParams {
    pub aggregate_functions: Vec<AggregateFunctionRef>,
    pub aggregate_functions_column_name: Vec<String>,
    pub aggregate_functions_arguments_name: Vec<Vec<String>>,
}

pub type AggregatorParamsRef = Arc<AggregatorParams>;

impl AggregatorParams {
    pub fn try_create(schema: DataSchemaRef, exprs: &[Expression]) -> Result<AggregatorParamsRef> {
        let mut aggregate_functions = Vec::with_capacity(exprs.len());
        let mut aggregate_functions_column_name = Vec::with_capacity(exprs.len());
        let mut aggregate_functions_arguments_name = Vec::with_capacity(exprs.len());

        for expr in exprs.iter() {
            aggregate_functions.push(expr.to_aggregate_function(&schema)?);
            aggregate_functions_column_name.push(expr.column_name());
            aggregate_functions_arguments_name.push(expr.to_aggregate_function_names()?);
        }

        Ok(Arc::new(AggregatorParams {
            aggregate_functions,
            aggregate_functions_column_name,
            aggregate_functions_arguments_name,
        }))
    }
}
