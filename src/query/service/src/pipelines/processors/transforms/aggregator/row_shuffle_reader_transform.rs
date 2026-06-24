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
use crate::pipelines::processors::transforms::aggregator::AggregateSpillReader;
use crate::pipelines::processors::transforms::aggregator::PartitionedData;

pub struct RowShuffleReaderTransform {
    reader: AggregateSpillReader,
}

impl RowShuffleReaderTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        reader: AggregateSpillReader,
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
        let payloads = match meta {
            AggregateMeta::Partitioned {
                data: PartitionedData::BucketSpilled(payloads),
                ..
            } => payloads,
            meta => return Ok(vec![DataBlock::empty_with_meta(Box::new(meta))]),
        };

        let restored = payloads
            .into_iter()
            .map(|payload| self.reader.restore(payload))
            .collect::<Result<_, _>>()?;
        let meta = AggregateMeta::Partitioned {
            bucket: None,
            data: PartitionedData::Serialized(restored),
        };
        Ok(vec![DataBlock::empty_with_meta(Box::new(meta))])
    }
}
