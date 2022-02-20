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

use std::alloc::Layout;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::AggregateFunctionRef;
use common_planners::Expression;

pub struct AggregatorParams {
    pub schema: DataSchemaRef,
    pub group_columns_name: Vec<String>,

    pub aggregate_functions: Vec<AggregateFunctionRef>,
    pub aggregate_functions_column_name: Vec<String>,
    pub aggregate_functions_arguments_name: Vec<Vec<String>>,

    // about function state memory layout
    pub layout: Layout,
    pub offsets_aggregate_states: Vec<usize>,
}

pub type AggregatorParamsRef = Arc<AggregatorParams>;

impl AggregatorParams {
    pub fn try_create(
        schema: &DataSchemaRef,
        before_schema: &DataSchemaRef,
        exprs: &[Expression],
        group_cols: &[String],
    ) -> Result<AggregatorParamsRef> {
        let mut aggregate_functions = Vec::with_capacity(exprs.len());
        let mut aggregate_functions_column_name = Vec::with_capacity(exprs.len());
        let mut aggregate_functions_arguments_name = Vec::with_capacity(exprs.len());

        for expr in exprs.iter() {
            aggregate_functions.push(expr.to_aggregate_function(before_schema)?);
            aggregate_functions_column_name.push(expr.column_name());
            aggregate_functions_arguments_name.push(expr.to_aggregate_function_names()?);
        }

        let (states_layout, states_offsets) = unsafe { get_layout_offsets(&aggregate_functions) };

        Ok(Arc::new(AggregatorParams {
            aggregate_functions,
            aggregate_functions_column_name,
            aggregate_functions_arguments_name,
            layout: states_layout,
            schema: schema.clone(),
            group_columns_name: group_cols.to_vec(),
            offsets_aggregate_states: states_offsets,
        }))
    }
}
