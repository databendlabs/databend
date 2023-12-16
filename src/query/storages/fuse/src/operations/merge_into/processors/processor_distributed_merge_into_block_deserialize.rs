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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;

use super::processor_merge_into_matched_and_split::MixRowIdKindAndLog;
use super::RowIdKind;

// It will receive MutationLogs Or RowIds.
// But for MutationLogs, it's a empty block
// we will add a fake BlockEntry to make it consistent with
// RowIds, because arrow-flight requires this.
pub struct TransformDistributedMergeIntoBlockDeserialize;

/// this processor will be used in the future for merge into based on shuffle hash join.
impl TransformDistributedMergeIntoBlockDeserialize {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>) -> ProcessorPtr {
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
