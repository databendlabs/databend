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

use bumpalo::Bump;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::BlockMetaTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaTransformer;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::estimated_key_size;
use crate::pipelines::processors::transforms::group_by::GroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::AggregatorParams;

pub struct TransformFinalGroupBy<Method: HashMethodBounds> {
    method: Method,
    params: Arc<AggregatorParams>,
}

impl<Method: HashMethodBounds> TransformFinalGroupBy<Method> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        params: Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        Ok(Box::new(BlockMetaTransformer::create(
            input,
            output,
            TransformFinalGroupBy::<Method> { method, params },
        )))
    }
}

impl<Method> BlockMetaTransform<AggregateMeta<Method, ()>> for TransformFinalGroupBy<Method>
where Method: HashMethodBounds
{
    const NAME: &'static str = "TransformFinalGroupBy";

    fn transform(&mut self, meta: AggregateMeta<Method, ()>) -> Result<DataBlock> {
        if let AggregateMeta::Partitioned { bucket, data } = meta {
            let arena = Arc::new(Bump::new());
            let mut hashtable = self.method.create_hash_table::<()>(arena)?;
            'merge_hashtable: for bucket_data in data {
                match bucket_data {
                    AggregateMeta::Spilled(_) => unreachable!(),
                    AggregateMeta::BucketSpilled(_) => unreachable!(),
                    AggregateMeta::Spilling(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::Serialized(payload) => {
                        debug_assert!(bucket == payload.bucket);
                        let column = payload.get_group_by_column();
                        let keys_iter = self.method.keys_iter_from_column(column)?;

                        unsafe {
                            for key in keys_iter.iter() {
                                let _ = hashtable.insert_and_entry(key);
                            }

                            if let Some(limit) = self.params.limit {
                                if hashtable.len() >= limit {
                                    break 'merge_hashtable;
                                }
                            }
                        }
                    }
                    AggregateMeta::HashTable(payload) => unsafe {
                        debug_assert!(bucket == payload.bucket);

                        for key in payload.cell.hashtable.iter() {
                            let _ = hashtable.insert_and_entry(key.key());
                        }

                        if let Some(limit) = self.params.limit {
                            if hashtable.len() >= limit {
                                break 'merge_hashtable;
                            }
                        }
                    },
                }
            }

            let value_size = estimated_key_size(&hashtable);
            let keys_len = hashtable.len();

            let mut group_columns_builder =
                self.method
                    .group_columns_builder(keys_len, value_size, &self.params);

            for group_entity in hashtable.iter() {
                group_columns_builder.append_value(group_entity.key());
            }

            return Ok(DataBlock::new_from_columns(group_columns_builder.finish()?));
        }

        Err(ErrorCode::Internal(
            "TransformFinalGroupBy only recv AggregateMeta::Partitioned",
        ))
    }
}
