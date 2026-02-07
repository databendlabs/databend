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
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::aggregates::AggregateFunctionSortDesc;
use databend_common_sql::IndexType;
use databend_common_sql::executor::physical_plans::AggregateFunctionDesc;
use databend_common_sql::plans::UDFType;
use itertools::Itertools;

use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::create_udaf_script_function;

impl PipelineBuilder {
    pub fn build_aggregator_params(
        input_schema: DataSchemaRef,
        group_by: &[IndexType],
        agg_funcs: &[AggregateFunctionDesc],
        cluster_aggregator: bool,
        max_spill_io_requests: usize,
        enable_experiment_aggregate: bool,
        enable_experiment_hash_index: bool,
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

        let aggs: Vec<AggregateFunctionRef> = agg_funcs
            .iter()
            .map(|agg_func| {
                let input_len = agg_func.arg_indices.len() + agg_func.sort_desc_indices.len();
                let mut arg_indexes = Vec::with_capacity(input_len);
                let mut args = Vec::with_capacity(input_len);

                for p in agg_func.arg_indices.iter() {
                    args.push(input_schema.index_of(&p.to_string())?);
                    arg_indexes.push(*p);
                }
                for (i, desc) in agg_func.sig.sort_descs.iter().enumerate() {
                    // sort_desc will reuse existing columns, so only need to insert new columns.
                    if agg_func.sig.sort_descs[i].is_reuse_index && args.contains(&desc.index) {
                        continue;
                    }
                    args.push(input_schema.index_of(&desc.index.to_string())?);
                    arg_indexes.push(desc.index);
                }
                agg_args.push(args);

                let remapping_sort_descs = agg_func
                    .sig
                    .sort_descs
                    .iter()
                    .map(|desc| {
                        let index = arg_indexes
                            .iter()
                            .find_position(|i| **i == desc.index)
                            .map(|(i, _)| i)
                            .unwrap_or(desc.index);
                        Ok(AggregateFunctionSortDesc {
                            index,
                            is_reuse_index: desc.is_reuse_index,
                            data_type: desc.data_type.clone(),
                            nulls_first: desc.nulls_first,
                            asc: desc.asc,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                match &agg_func.sig.udaf {
                    None => AggregateFunctionFactory::instance().get(
                        agg_func.sig.name.as_str(),
                        agg_func.sig.params.clone(),
                        agg_func.sig.args.clone(),
                        remapping_sort_descs,
                    ),
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
                }
            })
            .collect::<Result<_>>()?;

        let params = AggregatorParams::try_create(
            input_schema,
            group_data_types,
            &group_by,
            &aggs,
            &agg_args,
            cluster_aggregator,
            max_spill_io_requests,
            enable_experiment_aggregate,
            enable_experiment_hash_index,
            max_block_rows,
            max_block_bytes,
        )?;

        log::debug!("aggregate states layout: {:?}", params.states_layout);

        Ok(params)
    }
}
