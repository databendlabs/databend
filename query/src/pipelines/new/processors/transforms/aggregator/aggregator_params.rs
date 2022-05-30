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

use common_datablocks::DataBlock;
use common_datablocks::HashMethodKind;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::AggregateFunctionRef;
use common_planners::Expression;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;

pub struct AggregatorParams {
    pub schema: DataSchemaRef,
    pub before_schema: DataSchemaRef,
    pub group_columns_name: Vec<String>,
    pub group_data_fields: Vec<DataField>,

    pub aggregate_functions: Vec<AggregateFunctionRef>,
    pub aggregate_functions_column_name: Vec<String>,
    pub aggregate_functions_arguments_name: Vec<Vec<String>>,

    // about function state memory layout
    // If there is no aggregate function, layout is None
    pub layout: Option<Layout>,
    pub offsets_aggregate_states: Vec<usize>,
}

impl AggregatorParams {
    fn extract_group_columns(group_exprs: &[Expression]) -> Vec<String> {
        group_exprs
            .iter()
            .map(|x| x.column_name())
            .collect::<Vec<_>>()
    }

    pub fn try_create(
        aggr_expr: &[Expression],
        group_expr: &[Expression],
        input_schema: &DataSchemaRef,
        output_schema: &DataSchemaRef,
    ) -> Result<Arc<AggregatorParams>> {
        let group_cols = Self::extract_group_columns(group_expr);
        let mut aggregate_functions = Vec::with_capacity(aggr_expr.len());
        let mut aggregate_functions_column_name = Vec::with_capacity(aggr_expr.len());
        let mut aggregate_functions_arguments_name = Vec::with_capacity(aggr_expr.len());

        for expr in aggr_expr.iter() {
            aggregate_functions.push(expr.to_aggregate_function(input_schema)?);
            aggregate_functions_column_name.push(expr.column_name());
            aggregate_functions_arguments_name.push(expr.to_aggregate_function_names()?);
        }

        let mut states_offsets: Vec<usize> = Vec::with_capacity(aggregate_functions.len());
        let mut states_layout = None;
        if !aggregate_functions.is_empty() {
            states_offsets = Vec::with_capacity(aggregate_functions.len());
            states_layout = Option::Some(get_layout_offsets(
                aggregate_functions.as_slice(),
                &mut states_offsets,
            )?);
        }

        let group_data_fields = group_expr
            .iter()
            .map(|c| c.to_data_field(input_schema))
            .collect::<Result<Vec<_>>>()?;

        Ok(Arc::new(AggregatorParams {
            group_data_fields,
            aggregate_functions,
            aggregate_functions_column_name,
            aggregate_functions_arguments_name,
            layout: states_layout,
            schema: output_schema.clone(),
            before_schema: input_schema.clone(),
            group_columns_name: group_cols.to_vec(),
            offsets_aggregate_states: states_offsets,
        }))
    }
}

pub struct AggregatorTransformParams {
    pub method: HashMethodKind,
    pub transform_input_port: Arc<InputPort>,
    pub transform_output_port: Arc<OutputPort>,
    pub aggregator_params: Arc<AggregatorParams>,
}

impl AggregatorTransformParams {
    pub fn try_create(
        transform_input_port: Arc<InputPort>,
        transform_output_port: Arc<OutputPort>,
        aggregator_params: &Arc<AggregatorParams>,
    ) -> Result<AggregatorTransformParams> {
        let group_cols = &aggregator_params.group_columns_name;
        let schema_before_group_by = aggregator_params.before_schema.clone();
        let sample_block = DataBlock::empty_with_schema(schema_before_group_by);
        let method = DataBlock::choose_hash_method(&sample_block, group_cols)?;
        
        eprintln!("hash method -> {:?}", method.name());
        Ok(AggregatorTransformParams {
            method,
            transform_input_port,
            transform_output_port,
            aggregator_params: aggregator_params.clone(),
        })
    }
}
