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

use databend_common_exception::Result;
use databend_common_expression::simpler::Simpler;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use rand::rngs::StdRng;

use super::meta::SortShuffle;

pub struct TransformSortSimple {
    simpler: Simpler<StdRng>,
    blocks: Vec<DataBlock>,
}

impl TransformSortSimple {
    fn create() {}
}

impl AccumulatingTransform for TransformSortSimple {
    const NAME: &'static str = "TransformSortSimple";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        self.simpler.add_block(data.clone());
        self.blocks.push(data);

        Ok(vec![])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        self.simpler.compact_blocks();
        let mut simple = self.simpler.take_blocks();
        assert!(simple.len() <= 1);
        let simple = if simple.is_empty() {
            DataBlock::empty_with_meta(SortShuffle.boxed())
        } else {
            simple.remove(0).add_meta(Some(SortShuffle.boxed()))?
        };

        let blocks = std::mem::take(&mut self.blocks);

        Ok(std::iter::once(simple).chain(blocks).collect())
    }
}
