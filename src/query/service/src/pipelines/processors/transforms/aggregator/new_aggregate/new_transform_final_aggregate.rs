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

use std::mem;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_pipeline_transforms::AccumulatingTransformer;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::NewAggregateSpillReader;
use crate::pipelines::processors::transforms::aggregator::NewSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::pipelines::processors::transforms::aggregator::statistics::AggregationStatistics;
use crate::pipelines::processors::transforms::aggregator::transform_aggregate_partial::HashTable;
use crate::sessions::QueryContext;

pub struct NewTransformFinalAggregate {
    hashtable: HashTable,
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
    statistics: AggregationStatistics,
    _id: usize,
    reader: NewAggregateSpillReader,
}

impl NewTransformFinalAggregate {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        _id: usize,
        ctx: Arc<QueryContext>,
    ) -> Result<Box<dyn Processor>> {
        // TODO: the initial capacity is too small, can be optimized here
        let hashtable = AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            HashTableConfig::default().with_initial_radix_bits(0),
            Arc::new(Bump::new()),
        );
        let flush_state = PayloadFlushState::default();

        let reader = NewAggregateSpillReader::try_create(ctx)?;

        Ok(AccumulatingTransformer::create(
            input,
            output,
            NewTransformFinalAggregate {
                hashtable: HashTable::AggregateHashTable(hashtable),
                params,
                flush_state,
                statistics: AggregationStatistics::new("NewFinalAggregate"),
                _id,
                reader,
            },
        ))
    }
}

impl NewTransformFinalAggregate {
    fn handle_serialized(&mut self, payload: SerializedPayload) -> Result<()> {
        if payload.data_block.is_empty() {
            return Ok(());
        }

        let rows = payload.data_block.num_rows();
        let bytes = payload.data_block.memory_size();
        self.statistics.record_block(rows, bytes);

        let partitioned_payload = payload.convert_to_partitioned_payload(
            self.params.group_data_types.clone(),
            self.params.aggregate_functions.clone(),
            self.params.num_states(),
            0,
            Arc::new(Bump::new()),
        )?;

        if let HashTable::AggregateHashTable(ht) = &mut self.hashtable {
            ht.combine_payloads(&partitioned_payload, &mut self.flush_state)?;
        }

        Ok(())
    }

    fn handle_aggregate_payload(&mut self, payload: AggregatePayload) -> Result<()> {
        let rows = payload.payload.len();
        let bytes = payload.payload.memory_size();
        self.statistics.record_block(rows, bytes);

        if let HashTable::AggregateHashTable(ht) = &mut self.hashtable {
            ht.combine_payload(&payload.payload, &mut self.flush_state)?;
        }

        Ok(())
    }

    fn handle_new_spilled(&mut self, payloads: Vec<NewSpilledPayload>) -> Result<()> {
        for payload in payloads {
            let restored = self.reader.restore(payload)?;
            let AggregateMeta::Serialized(restored) = restored else {
                unreachable!("unexpected aggregate meta, found type: {:?}", restored)
            };
            self.handle_serialized(restored)?;
        }

        Ok(())
    }

    fn handle_meta(&mut self, meta: AggregateMeta) -> Result<()> {
        match meta {
            AggregateMeta::Serialized(payload) => {
                self.handle_serialized(payload)?;
            }
            AggregateMeta::AggregatePayload(payload) => {
                self.handle_aggregate_payload(payload)?;
            }
            AggregateMeta::NewSpilled(payloads) => {
                self.handle_new_spilled(payloads)?;
            }
            AggregateMeta::NewBucketSpilled(payload) => {
                self.handle_new_spilled(vec![payload])?;
            }
            AggregateMeta::Partitioned { bucket: _, data } => {
                for meta in data {
                    self.handle_meta(meta)?;
                }
            }
            _ => {
                unreachable!("unexpected aggregate meta, found type: {:?}", meta);
            }
        }

        Ok(())
    }
}

impl AccumulatingTransform for NewTransformFinalAggregate {
    const NAME: &'static str = "NewTransformFinalAggregate";

    fn transform(&mut self, mut data: DataBlock) -> Result<Vec<DataBlock>> {
        if let Some(meta) = data.take_meta() {
            if let Some(meta) = AggregateMeta::downcast_from(meta) {
                self.handle_meta(meta)?;
                return Ok(vec![]);
            }
        }
        unreachable!("unexpected datablock")
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        if let HashTable::AggregateHashTable(mut ht) = mem::take(&mut self.hashtable) {
            self.statistics.log_finish_statistics(&ht);
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

            if !blocks.is_empty() {
                let concat = DataBlock::concat(&blocks)?;
                if !concat.is_empty() {
                    return Ok(vec![concat]);
                }
            }
        }

        Ok(vec![])
    }
}
