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
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::types::ArgType;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_pipeline_transforms::AccumulatingTransform;

use crate::pipelines::processors::transforms::RangeBoundSampler;

pub struct TransformReclusterCollect<T>
where
    T: ArgType + Send + Sync,
    T::Scalar: Ord + Send,
{
    input_data: Vec<DataBlock>,
    sampler: RangeBoundSampler<T>,
}

impl<T> TransformReclusterCollect<T>
where
    T: ArgType + Send + Sync,
    T::Scalar: Ord + Send,
{
    pub fn new(offset: usize, sample_size: usize, seed: u64) -> Self {
        Self {
            input_data: vec![],
            sampler: RangeBoundSampler::<T>::new(offset, sample_size, seed),
        }
    }
}

impl<T> AccumulatingTransform for TransformReclusterCollect<T>
where
    T: ArgType + Send + Sync,
    T::Scalar: Ord + Send,
{
    const NAME: &'static str = "TransformReclusterCollect";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        self.sampler.add_block(&data);
        self.input_data.push(data);
        Ok(vec![])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        let sample_values = self.sampler.sample_values();
        let blocks = std::mem::take(&mut self.input_data);
        let meta = ReclusterSampleMeta {
            blocks,
            sample_values,
        };

        Ok(vec![DataBlock::empty_with_meta(Box::new(meta))])
    }
}

#[derive(Debug)]
pub struct ReclusterSampleMeta {
    pub blocks: Vec<DataBlock>,
    pub sample_values: Vec<(u64, Vec<Scalar>)>,
}

local_block_meta_serde!(ReclusterSampleMeta);

#[typetag::serde(name = "recluster_sample")]
impl BlockMetaInfo for ReclusterSampleMeta {}
