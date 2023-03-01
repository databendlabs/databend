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

use common_expression::DataBlock;
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
use crate::pipelines::processors::transforms::aggregator::serde::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;

pub struct TransformGroupBySerializer<Method: HashMethodBounds> {
    method: Method,
}

impl<Method: HashMethodBounds> TransformGroupBySerializer<Method> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
    ) -> common_exception::Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformGroupBySerializer { method },
        )))
    }
}

impl<Method> BlockMetaTransform<AggregateMeta<Method, ()>> for TransformGroupBySerializer<Method>
where Method: HashMethodBounds
{
    const NAME: &'static str = "TransformGroupBySerializer";

    fn transform(
        &mut self,
        meta: AggregateMeta<Method, ()>,
    ) -> common_exception::Result<DataBlock> {
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
                let data_block = serialize_group_by(&self.method, payload)?;
                data_block.add_meta(Some(AggregateSerdeMeta::create(bucket)))
            }
        }
    }
}

pub fn serialize_group_by<Method: HashMethodBounds>(
    method: &Method,
    payload: HashTablePayload<Method, ()>,
) -> common_exception::Result<DataBlock> {
    let keys_len = payload.cell.hashtable.len();
    let value_size = estimated_key_size(&payload.cell.hashtable);
    let mut group_key_builder = method.keys_column_builder(keys_len, value_size);

    for group_entity in payload.cell.hashtable.iter() {
        group_key_builder.append_value(group_entity.key());
    }

    Ok(DataBlock::new_from_columns(vec![
        group_key_builder.finish(),
    ]))
}
