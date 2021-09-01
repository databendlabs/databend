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

use std::alloc::Layout;

use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datavalues::arrays::BinaryArrayBuilder;
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::IntoSeries;
use common_datavalues::prelude::Series;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::StateAddr;
use common_infallible::Mutex;
use common_io::prelude::BytesMut;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::pipelines::transforms::group_by::aggregator_keys_builder::KeysArrayBuilder;
use crate::pipelines::transforms::group_by::aggregator_params::AggregatorParams;
use crate::pipelines::transforms::group_by::aggregator_params::AggregatorParamsRef;
use crate::pipelines::transforms::group_by::aggregator_state::AggregatorState;
use crate::pipelines::transforms::group_by::aggregator_state_entity::StateEntity;
use crate::pipelines::transforms::group_by::PolymorphicKeysHelper;

pub struct Aggregator<Method: HashMethod> {
    method: Method,
    params: AggregatorParamsRef,
    layout: Layout,
    offsets_aggregate_states: Vec<usize>,
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method>> Aggregator<Method> {
    pub fn create(method: Method, params: AggregatorParamsRef) -> Aggregator<Method> {
        let aggregate_functions = &params.aggregate_functions;
        let (states_layout, states_offsets) = unsafe { get_layout_offsets(aggregate_functions) };

        Aggregator {
            method,
            params,
            layout: states_layout,
            offsets_aggregate_states: states_offsets,
        }
    }

    #[inline(never)]
    pub async fn aggregate(&self, group_cols: Vec<String>, mut stream: SendableDataBlockStream) -> Result<Mutex<Method::State>> {
        let hash_method = &self.method;

        let groups_locker = Mutex::new(hash_method.aggregate_state());

        let aggregator_params = self.params.as_ref();

        let aggr_len = aggregator_params.aggregate_functions.len();
        let aggregate_functions = &aggregator_params.aggregate_functions;
        let aggregate_functions_arguments = &aggregator_params.aggregate_functions_arguments_name;

        let layout = self.layout;
        let offsets_aggregate_states = &self.offsets_aggregate_states;

        while let Some(block) = stream.next().await {
            let block = block?;

            // 1.1 and 1.2.
            let group_columns = Self::group_columns(&group_cols, &block)?;
            let mut aggregate_arguments_columns = Vec::with_capacity(aggr_len);

            {
                for function_arguments in aggregate_functions_arguments {
                    let mut function_arguments_column =
                        Vec::with_capacity(function_arguments.len());

                    for argument_name in function_arguments {
                        let argument_column = block.try_column_by_name(argument_name)?;
                        function_arguments_column.push(argument_column.to_array()?);
                    }

                    aggregate_arguments_columns.push(function_arguments_column);
                }
            }

            let mut places = Vec::with_capacity(block.num_rows());
            let group_keys = hash_method.build_keys(&group_columns, block.num_rows())?;
            let mut groups = groups_locker.lock();
            {
                let mut inserted = true;
                for group_key in group_keys.iter() {
                    let entity = groups.entity(group_key, &mut inserted);

                    match inserted {
                        true => {
                            if aggr_len == 0 {
                                entity.set_state_value(0);
                            } else {
                                let place: StateAddr = groups.alloc_layout(layout).into();
                                for idx in 0..aggr_len {
                                    let aggr_state = offsets_aggregate_states[idx];
                                    let aggr_state_place = place.next(aggr_state);
                                    aggregate_functions[idx].init_state(aggr_state_place);
                                }
                                places.push(place);
                                entity.set_state_value(place.addr());
                            }
                        }
                        false => {
                            let place: StateAddr = (*entity.get_state_value()).into();
                            places.push(place);
                        }
                    }
                }
            }

            {
                // this can benificial for the case of dereferencing
                let aggr_arg_columns_slice = &aggregate_arguments_columns;

                for ((idx, func), args) in aggregate_functions
                    .iter()
                    .enumerate()
                    .zip(aggr_arg_columns_slice.iter())
                {
                    func.accumulate_keys(
                        &places,
                        offsets_aggregate_states[idx],
                        args,
                        block.num_rows(),
                    )?;
                }
            }
        }
        Ok(groups_locker)
    }

    #[inline(always)]
    fn group_columns<'a>(names: &[String], block: &'a DataBlock) -> Result<Vec<&'a DataColumn>> {
        names
            .iter()
            .map(|column_name| block.try_column_by_name(column_name))
            .collect::<Result<Vec<&DataColumn>>>()
    }

    pub fn aggregate_finalized(
        &self,
        groups: &Method::State,
        schema: DataSchemaRef,
    ) -> Result<SendableDataBlockStream> {
        if groups.len() == 0 {
            return Ok(Box::pin(DataBlockStream::create(
                DataSchemaRefExt::create(vec![]),
                None,
                vec![],
            )));
        }

        let aggregator_params = self.params.as_ref();
        let funcs = &aggregator_params.aggregate_functions;
        let aggr_len = funcs.len();
        let offsets_aggregate_states = &self.offsets_aggregate_states;

        // Builders.
        let mut state_builders: Vec<BinaryArrayBuilder> = (0..aggr_len)
            .map(|_| BinaryArrayBuilder::with_capacity(groups.len() * 4))
            .collect();

        let mut group_key_builder = self.method.state_array_builder(groups.len());

        let mut bytes = BytesMut::new();
        for group_entity in groups.iter() {
            let place: StateAddr = (*group_entity.get_state_value()).into();

            for (idx, func) in funcs.iter().enumerate() {
                let arg_place = place.next(offsets_aggregate_states[idx]);
                func.serialize(arg_place, &mut bytes)?;
                state_builders[idx].append_value(&bytes[..]);
                bytes.clear();
            }

            group_key_builder.append_value(group_entity.get_state_key());
        }

        let mut columns: Vec<Series> = Vec::with_capacity(schema.fields().len());
        for mut builder in state_builders {
            columns.push(builder.finish().into_series());
        }

        columns.push(group_key_builder.finish());

        let block = DataBlock::create_by_array(schema.clone(), columns);
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }
}
