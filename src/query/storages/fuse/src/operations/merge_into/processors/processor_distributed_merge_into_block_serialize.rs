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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType::UInt64;
use databend_common_expression::types::NumberType;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;

use super::processor_merge_into_matched_and_split::MixRowIdKindAndLog;
use super::RowIdKind;
use crate::operations::common::MutationLogs;

// It will receive MutationLogs Or RowIds.
// But for MutationLogs, it's a empty block
// we will add a fake BlockEntry to make it consistent with
// RowIds, because arrow-flight requires this.
pub struct TransformDistributedMergeIntoBlockSerialize;

/// this processor will be used in the future for merge into based on shuffle hash join.
impl TransformDistributedMergeIntoBlockSerialize {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>) -> ProcessorPtr {
        ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformDistributedMergeIntoBlockSerialize {},
        ))
    }

    fn create_distributed_merge_into_transform_item() -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        PipeItem::create(
            TransformDistributedMergeIntoBlockSerialize::create(input.clone(), output.clone()),
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
impl Transform for TransformDistributedMergeIntoBlockSerialize {
    const NAME: &'static str = "TransformDistributedMergeIntoBlockSerialize";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        // 1. MutationLogs
        if data.is_empty() {
            let scalar_value = Value::<NumberType<u64>>::Scalar(0);
            let entry = BlockEntry::new(DataType::Number(UInt64), scalar_value.upcast());
            let log = MutationLogs::try_from(data)?;
            Ok(DataBlock::new_with_meta(
                vec![entry],
                1,
                Some(Box::new(MixRowIdKindAndLog {
                    log: Some(log),
                    kind: 0,
                })),
            ))
        } else {
            // RowIdKind
            let row_id_kind = RowIdKind::downcast_ref_from(data.get_meta().unwrap()).unwrap();
            Ok(DataBlock::new_with_meta(
                data.columns().to_vec(),
                data.num_rows(),
                Some(Box::new(MixRowIdKindAndLog {
                    log: None,
                    kind: match row_id_kind {
                        RowIdKind::Update => 1,
                        RowIdKind::Delete => 2,
                    },
                })),
            ))
        }
    }
}
