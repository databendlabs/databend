// Copyright 2021 Datafuse Labs
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

use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::aggregate::aggregate_function::AggregateCallRef;
use databend_common_expression::aggregate::aggregate_function::RawAggregateCall;
use databend_common_expression::aggregate_function::AggregateBoundOrderBySource;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use databend_common_sql::Symbol;
use databend_common_sql::executor::physical_plans::AggregateFunctionDesc;
use databend_common_sql::plans::UDFType;

use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::create_udaf_script_function;

impl PipelineBuilder {
    pub fn build_aggregator_params(
        input_schema: DataSchemaRef,
        group_by: &[Symbol],
        agg_funcs: &[AggregateFunctionDesc],
        cluster_aggregator: bool,
        max_block_rows: usize,
        max_block_bytes: usize,
    ) -> Result<Arc<AggregatorParams>> {
        let mut agg_args = Vec::with_capacity(agg_funcs.len());
        let (group_by, group_data_types) = group_by
            .iter()
            .map(|i| {
                let index = input_schema.index_of(&i.to_string())?;
                Ok((index, input_schema.field(index).data_type().clone()))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let aggs: Vec<AggregateCallRef> = agg_funcs
            .iter()
            .map(|agg_func| {
                let input_len = agg_func.arg_indices.len() + agg_func.sig.order_by.len();
                let mut args = Vec::with_capacity(input_len);

                for p in agg_func.arg_indices.iter() {
                    args.push(input_schema.index_of(&p.to_string())?);
                }
                for item in &agg_func.sig.order_by {
                    if matches!(item.source, AggregateBoundOrderBySource::Derived) {
                        args.push(input_schema.index_of(&item.index.to_string())?);
                    }
                }
                let function = match &agg_func.sig.udaf {
                    None => AGGR_REGISTRY.resolve(RawAggregateCall {
                        name: agg_func.sig.name.as_str(),
                        params: &agg_func.sig.params.clone(),
                        args_type: &agg_func.sig.args.clone(),
                        distinct: false,
                        order_by: &agg_func.sig.order_by,
                    }),
                    Some((UDFType::Script(code), state_fields)) => create_udaf_script_function(
                        code,
                        agg_func.sig.name.clone(),
                        agg_func.display.clone(),
                        state_fields
                            .iter()
                            .map(|f| DataField::new(&f.name, f.data_type.clone()))
                            .collect(),
                        agg_func
                            .sig
                            .args
                            .iter()
                            .enumerate()
                            .map(|(i, data_type)| {
                                DataField::new(&format!("arg_{}", i), data_type.clone())
                            })
                            .collect(),
                        agg_func.sig.return_type.clone(),
                    ),
                    Some((UDFType::Server(_), _state_fields)) => unimplemented!(),
                }?;
                let args = function.input_layout().project(&args)?.into_owned();
                agg_args.push(args);
                Ok(function)
            })
            .collect::<Result<_>>()?;

        let params = AggregatorParams::try_create(
            input_schema,
            group_data_types,
            &group_by,
            &aggs,
            &agg_args,
            cluster_aggregator,
            max_block_rows,
            max_block_bytes,
        )?;

        log::debug!("aggregate states layout: {:?}", params.states_layout);

        Ok(params)
    }
}
