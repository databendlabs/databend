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

use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::BlockMetaTransform;
use databend_common_pipeline_transforms::BlockMetaTransformer;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::NewAggregateSpillReader;

pub struct RowShuffleReaderTransform {
    reader: NewAggregateSpillReader,
}

impl RowShuffleReaderTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        reader: NewAggregateSpillReader,
    ) -> Box<dyn Processor> {
        BlockMetaTransformer::create(input, output, Self { reader })
    }
}

impl BlockMetaTransform<AggregateMeta> for RowShuffleReaderTransform {
    const NAME: &'static str = "RowShuffleReaderTransform";

    fn transform(
        &mut self,
        meta: AggregateMeta,
    ) -> databend_common_exception::Result<Vec<DataBlock>> {
        // We only processed Partitioned meta with new spilled buckets in it.
        if let AggregateMeta::Partitioned { data, .. } = &meta {
            let first = data.first();
            if !matches!(first, Some(AggregateMeta::NewBucketSpilled(_))) {
                return Ok(vec![DataBlock::empty_with_meta(Box::new(meta))]);
            }
        } else {
            return Ok(vec![DataBlock::empty_with_meta(Box::new(meta))]);
        }

        if let AggregateMeta::Partitioned { data, .. } = meta {
            let mut restored = Vec::with_capacity(data.len());
            for partition_meta in data {
                if let AggregateMeta::NewBucketSpilled(spilled_payload) = partition_meta {
                    let payload = self.reader.restore(spilled_payload)?;
                    restored.push(payload);
                } else {
                    return Err(databend_common_exception::ErrorCode::Internal(
                        "Unexpected aggregate meta in RowShuffleReaderTransform",
                    ));
                }
            }
            return Ok(vec![DataBlock::empty_with_meta(
                AggregateMeta::create_partitioned(None, restored),
            )]);
        } else {
            unreachable!()
        }
    }
}
