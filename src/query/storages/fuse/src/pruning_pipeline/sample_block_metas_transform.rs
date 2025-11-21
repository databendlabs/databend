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

use std::cmp::max;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransform;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransformer;
use databend_storages_common_table_meta::meta::BlockMeta;
use rand::distributions::Bernoulli;
use rand::distributions::Distribution;
use rand::seq::SliceRandom;
use rand::thread_rng;

use crate::pruning_pipeline::block_metas_meta::BlockMetasMeta;
const SMALL_DATASET_SAMPLE_THRESHOLD: usize = 100;

pub struct SampleBlockMetasTransform {
    probability: f64,
}

impl SampleBlockMetasTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        probability: f64,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(
            BlockMetaAccumulatingTransformer::create(input, output, SampleBlockMetasTransform {
                probability,
            }),
        ))
    }
}

impl BlockMetaAccumulatingTransform<BlockMetasMeta> for SampleBlockMetasTransform {
    const NAME: &'static str = "SampleBlockMetasTransform";

    fn transform(&mut self, data: BlockMetasMeta) -> Result<Option<DataBlock>> {
        let sample_block_metas = self.sample_block_metas(&data.block_metas);
        Ok(Some(DataBlock::empty_with_meta(BlockMetasMeta::create(
            sample_block_metas,
            data.segment_location,
        ))))
    }
}

impl SampleBlockMetasTransform {
    fn sample_block_metas(
        &self,
        block_metas: &Arc<Vec<Arc<BlockMeta>>>,
    ) -> Arc<Vec<Arc<BlockMeta>>> {
        if block_metas.len() <= SMALL_DATASET_SAMPLE_THRESHOLD {
            // Deterministic sampling for small datasets
            // Ensure at least one block is sampled for small datasets
            let sample_size = max(
                1,
                (block_metas.len() as f64 * self.probability).round() as usize,
            );
            let mut rng = thread_rng();
            Arc::new(
                block_metas
                    .choose_multiple(&mut rng, sample_size)
                    .cloned()
                    .collect(),
            )
        } else {
            // Random sampling for larger datasets
            let mut sample_block_metas = Vec::with_capacity(block_metas.len());
            let mut rng = thread_rng();
            let bernoulli = Bernoulli::new(self.probability).unwrap();
            for block in block_metas.iter() {
                if bernoulli.sample(&mut rng) {
                    sample_block_metas.push(block.clone());
                }
            }
            // Ensure at least one block is sampled for large datasets too
            if sample_block_metas.is_empty() && !block_metas.is_empty() {
                // Safe to unwrap, because we've checked that block_metas is not empty
                sample_block_metas.push(block_metas.choose(&mut rng).unwrap().clone());
            }
            Arc::new(sample_block_metas)
        }
    }
}
