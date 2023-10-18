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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

use super::processor_merge_into_matched_and_split::MixRowIdKindAndLog;
use super::RowIdKind;

// It will receive MutationLogs Or RowIds.
// But for MutationLogs, it's a empty block
// we will add a fake BlockEntry to make it consistent with
// RowIds, because arrow-flight requires this.
pub struct TransformDistributedMergeIntoBlockDeserialize;

impl TransformDistributedMergeIntoBlockDeserialize {
    #[allow(dead_code)]
    fn create(input: Arc<InputPort>, output: Arc<OutputPort>) -> ProcessorPtr {
        ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformDistributedMergeIntoBlockDeserialize {},
        ))
    }

    fn create_distributed_merge_into_transform_item() -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        PipeItem::create(
            TransformDistributedMergeIntoBlockDeserialize::create(input.clone(), output.clone()),
            vec![input],
            vec![output],
        )
    }

    pub fn into_pipe() -> Pipe {
        let pipe_item = Self::create_distributed_merge_into_transform_item();
        Pipe::create(1, 1, vec![pipe_item])
    }
}

#[async_trait::async_trait]
impl Transform for TransformDistributedMergeIntoBlockDeserialize {
    const NAME: &'static str = "TransformDistributedMergeIntoBlockDeserialize";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let mix_kind = MixRowIdKindAndLog::downcast_ref_from(data.get_meta().unwrap()).unwrap();
        match mix_kind.kind {
            0 => Ok(DataBlock::new_with_meta(
                data.columns().to_vec(),
                data.num_rows(),
                Some(Box::new(mix_kind.log.clone().unwrap())),
            )),

            1 => Ok(DataBlock::new_with_meta(
                data.columns().to_vec(),
                data.num_rows(),
                Some(Box::new(RowIdKind::Update)),
            )),
            2 => Ok(DataBlock::new_with_meta(
                data.columns().to_vec(),
                data.num_rows(),
                Some(Box::new(RowIdKind::Delete)),
            )),
            _ => Err(ErrorCode::BadBytes("get error MixRowIdKindAndLog kind")),
        }
    }
}
