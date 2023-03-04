// Copyright 2023 Datafuse Labs.
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

use common_exception::Result;
use common_expression::types::string::StringColumnBuilder;
use common_expression::Column;
use common_expression::DataBlock;
use common_functions::aggregates::StateAddr;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::BlockMetaTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaTransformer;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::HashTablePayload;
use crate::pipelines::processors::transforms::aggregator::estimated_key_size;
use crate::pipelines::processors::transforms::aggregator::serde::serde_meta::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;
use crate::pipelines::processors::AggregatorParams;

pub struct TransformAggregateSerializer<Method: HashMethodBounds> {
    method: Method,
    params: Arc<AggregatorParams>,
}

impl<Method: HashMethodBounds> TransformAggregateSerializer<Method> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        params: Arc<AggregatorParams>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformAggregateSerializer { method, params },
        )))
    }
}

impl<Method> BlockMetaTransform<AggregateMeta<Method, usize>>
    for TransformAggregateSerializer<Method>
where Method: HashMethodBounds
{
    const NAME: &'static str = "TransformAggregateSerializer";

    fn transform(&mut self, meta: AggregateMeta<Method, usize>) -> Result<DataBlock> {
        match meta {
            AggregateMeta::Spilling(_) => unreachable!(),
            AggregateMeta::Partitioned { .. } => unreachable!(),
            AggregateMeta::Serialized(_) => unreachable!(),
            AggregateMeta::Spilled(payload) => Ok(DataBlock::empty_with_meta(
                AggregateSerdeMeta::create_spilled(
                    payload.bucket,
                    payload.location,
                    payload.columns_layout,
                ),
            )),
            AggregateMeta::HashTable(payload) => {
                let bucket = payload.bucket;
                let data_block = serialize_aggregate(&self.method, &self.params, payload)?;
                // serialize_block(bucket)
                data_block.add_meta(Some(AggregateSerdeMeta::create(bucket)))
            }
        }
    }
}

pub fn serialize_aggregate<Method: HashMethodBounds>(
    method: &Method,
    params: &Arc<AggregatorParams>,
    payload: HashTablePayload<Method, usize>,
) -> Result<DataBlock> {
    let keys_len = payload.cell.hashtable.len();
    let value_size = estimated_key_size(&payload.cell.hashtable);

    let funcs = &params.aggregate_functions;
    let offsets_aggregate_states = &params.offsets_aggregate_states;

    // Builders.
    let mut state_builders = (0..funcs.len())
        .map(|_| StringColumnBuilder::with_capacity(keys_len, keys_len * 4))
        .collect::<Vec<_>>();

    let mut group_key_builder = method.keys_column_builder(keys_len, value_size);

    for group_entity in payload.cell.hashtable.iter() {
        let place = Into::<StateAddr>::into(*group_entity.get());

        for (idx, func) in funcs.iter().enumerate() {
            let arg_place = place.next(offsets_aggregate_states[idx]);
            func.serialize(arg_place, &mut state_builders[idx].data)?;
            state_builders[idx].commit_row();
        }

        group_key_builder.append_value(group_entity.key());
    }

    let mut columns = Vec::with_capacity(state_builders.len() + 1);

    for builder in state_builders.into_iter() {
        columns.push(Column::String(builder.build()));
    }

    columns.push(group_key_builder.finish());
    Ok(DataBlock::new_from_columns(columns))
}
