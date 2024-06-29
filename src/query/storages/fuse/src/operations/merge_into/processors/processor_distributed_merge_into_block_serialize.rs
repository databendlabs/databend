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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType::UInt64;
use databend_common_expression::types::NumberType;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_common_pipeline_transforms::processors::Transform;

use super::processor_merge_into_matched_and_split::MixRowIdKindAndLog;
use super::RowIdKind;
use crate::operations::common::MutationLogs;

// It will receive MutationLogs Or RowIds.
// But for MutationLogs, it's a empty block
// we will add a fake BlockEntry to make it consistent with
// RowIds, because arrow-flight requires this.
pub struct TransformDistributedMergeIntoBlockSerialize;

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
