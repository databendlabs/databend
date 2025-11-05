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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::MAX_AGGREGATE_HASHTABLE_BUCKETS_NUM;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_pipeline_transforms::AccumulatingTransformer;

use crate::pipelines::processors::transforms::aggregator::new_aggregate::NewAggregateSpiller;
use crate::pipelines::processors::transforms::aggregator::new_aggregate::SharedPartitionStream;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::sessions::QueryContext;

pub struct NewTransformAggregateSpillWriter {
    pub spiller: NewAggregateSpiller,
}

impl NewTransformAggregateSpillWriter {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        ctx: Arc<QueryContext>,
        shared_partition_stream: SharedPartitionStream,
    ) -> Result<Box<dyn Processor>> {
        let partition_count = MAX_AGGREGATE_HASHTABLE_BUCKETS_NUM as usize;
        let spiller =
            NewAggregateSpiller::try_create(ctx, partition_count, shared_partition_stream)?;

        Ok(AccumulatingTransformer::create(
            input,
            output,
            NewTransformAggregateSpillWriter { spiller },
        ))
    }
}

impl AccumulatingTransform for NewTransformAggregateSpillWriter {
    const NAME: &'static str = "NewTransformAggregateSpillWriter";

    fn transform(&mut self, mut data: DataBlock) -> Result<Vec<DataBlock>> {
        if let Some(block_meta) = data.get_meta().and_then(AggregateMeta::downcast_ref_from) {
            if matches!(block_meta, AggregateMeta::AggregateSpilling(_)) {
                let meta = data.take_meta().unwrap();
                let aggregate_meta = AggregateMeta::downcast_from(meta).unwrap();
                if let AggregateMeta::AggregateSpilling(partition) = aggregate_meta {
                    for (bucket, payload) in partition.payloads.into_iter().enumerate() {
                        if payload.len() == 0 {
                            continue;
                        }

                        let data_block = payload.aggregate_flush_all()?.consume_convert_to_full();
                        self.spiller.spill(bucket, data_block)?;
                    }
                }
                return Ok(vec![]);
            }
        }

        Ok(vec![data])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        let spilled_payloads = self.spiller.spill_finish()?;

        let mut spilled_blocks = Vec::with_capacity(spilled_payloads.len());
        for payload in spilled_payloads {
            spilled_blocks.push(DataBlock::empty_with_meta(
                AggregateMeta::create_new_spilled(payload),
            ));
        }

        Ok(spilled_blocks)
    }
}
