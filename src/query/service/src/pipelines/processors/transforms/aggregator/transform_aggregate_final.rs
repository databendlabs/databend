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
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;

use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;

pub struct TransformFinalAggregate {
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
}

impl TransformFinalAggregate {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,

        params: Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        Ok(BlockMetaTransformer::create(
            input,
            output,
            TransformFinalAggregate {
                params,
                flush_state: PayloadFlushState::default(),
            },
        ))
    }

    fn transform_agg_hashtable(&mut self, meta: AggregateMeta) -> Result<DataBlock> {
        let mut agg_hashtable: Option<AggregateHashTable> = None;
        if let AggregateMeta::Partitioned { bucket, data, .. } = meta {
            let bucket = bucket.expect("final aggregate should have bucket info");
            for bucket_data in data {
                match bucket_data {
                    AggregateMeta::Serialized(payload) => match agg_hashtable.as_mut() {
                        Some(ht) => {
                            debug_assert!(bucket == payload.bucket);

                            let payload = payload.convert_to_partitioned_payload(
                                self.params.group_data_types.clone(),
                                self.params.aggregate_functions.clone(),
                                self.params.num_states(),
                                0,
                                self.params.enable_experiment_hash_index,
                                Arc::new(Bump::new()),
                            )?;
                            ht.combine_payloads(&payload, &mut self.flush_state)?;
                        }
                        None => {
                            debug_assert!(bucket == payload.bucket);
                            agg_hashtable = Some(payload.convert_to_aggregate_table(
                                self.params.group_data_types.clone(),
                                self.params.aggregate_functions.clone(),
                                self.params.num_states(),
                                0,
                                self.params.enable_experiment_hash_index,
                                Arc::new(Bump::new()),
                                true,
                            )?);
                        }
                    },
                    AggregateMeta::AggregatePayload(payload) => match agg_hashtable.as_mut() {
                        Some(ht) => {
                            debug_assert!(bucket == payload.bucket);
                            ht.combine_payload(&payload.payload, &mut self.flush_state)?;
                        }
                        None => {
                            debug_assert!(bucket == payload.bucket);
                            let capacity =
                                AggregateHashTable::get_capacity_for_count(payload.payload.len());
                            let mut hashtable = AggregateHashTable::new_with_capacity(
                                self.params.group_data_types.clone(),
                                self.params.aggregate_functions.clone(),
                                HashTableConfig::default()
                                    .with_initial_radix_bits(0)
                                    .with_experiment_hash_index(
                                        self.params.enable_experiment_hash_index,
                                    ),
                                capacity,
                                Arc::new(Bump::new()),
                            );
                            hashtable.combine_payload(&payload.payload, &mut self.flush_state)?;
                            agg_hashtable = Some(hashtable);
                        }
                    },
                    AggregateMeta::NewSpilled(_) => unreachable!(),
                    _ => unreachable!(),
                }
            }
        }

        if let Some(mut ht) = agg_hashtable {
            let mut blocks = vec![];
            self.flush_state.clear();

            loop {
                if ht.merge_result(&mut self.flush_state)? {
                    let mut entries = self.flush_state.take_aggregate_results();
                    let group_columns = self.flush_state.take_group_columns();
                    entries.extend_from_slice(&group_columns);
                    let num_rows = entries[0].len();
                    blocks.push(DataBlock::new(entries, num_rows));
                } else {
                    break;
                }
            }

            if blocks.is_empty() {
                return Ok(self.params.empty_result_block());
            }
            return DataBlock::concat(&blocks);
        }

        Ok(self.params.empty_result_block())
    }
}

impl BlockMetaTransform<AggregateMeta> for TransformFinalAggregate {
    const NAME: &'static str = "TransformFinalAggregate";

    fn transform(&mut self, meta: AggregateMeta) -> Result<Vec<DataBlock>> {
        Ok(vec![self.transform_agg_hashtable(meta)?])
    }
}
