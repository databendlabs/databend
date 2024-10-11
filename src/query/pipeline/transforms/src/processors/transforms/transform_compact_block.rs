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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::intrinsics::unlikely;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;

use crate::processors::BlockMetaTransform;
use crate::processors::UnknownMode;

pub enum BlockCompactMeta {
    Concat(Vec<DataBlock>),
    Split {
        block: DataBlock,
        rows_per_block: usize,
    },
    NoChange(Vec<DataBlock>),
}

impl Debug for BlockCompactMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("BlockCompactMeta").finish()
    }
}

impl serde::Serialize for BlockCompactMeta {
    fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize BlockCompactMeta")
    }
}

impl<'de> serde::Deserialize<'de> for BlockCompactMeta {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize BlockCompactMeta")
    }
}

#[typetag::serde(name = "block_compact")]
impl BlockMetaInfo for BlockCompactMeta {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals BlockCompactMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone BlockCompactMeta")
    }
}

#[derive(Default)]
pub struct TransformCompactBlock {
    aborting: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl BlockMetaTransform<BlockCompactMeta> for TransformCompactBlock {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Pass;
    const NAME: &'static str = "TransformCompactBlock";

    fn transform(&mut self, meta: BlockCompactMeta) -> Result<Vec<DataBlock>> {
        if unlikely(self.aborting.load(Ordering::Relaxed)) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        match meta {
            BlockCompactMeta::Concat(blocks) => Ok(vec![DataBlock::concat(&blocks)?]),
            BlockCompactMeta::Split {
                block,
                rows_per_block,
            } => Ok(block.split_by_rows_if_needed_no_tail(rows_per_block)),
            BlockCompactMeta::NoChange(blocks) => Ok(blocks),
        }
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
    }
}
