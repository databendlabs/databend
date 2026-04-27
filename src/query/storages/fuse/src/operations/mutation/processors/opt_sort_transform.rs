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
use databend_common_expression::DataBlock;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_transforms::Transform;

use crate::operations::ReclusterSourceMeta;

pub struct TransformOptSortPartial {
    sort_desc: Arc<[SortColumnDescription]>,
}

impl TransformOptSortPartial {
    pub fn new(sort_desc: Arc<[SortColumnDescription]>) -> Self {
        Self { sort_desc }
    }
}

impl Transform for TransformOptSortPartial {
    const NAME: &'static str = "TransformOptSortPartial";

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        let need_local_sort = block
            .get_meta()
            .and_then(ReclusterSourceMeta::from_meta)
            .is_some_and(|meta| meta.need_local_sort);
        if !need_local_sort {
            return Ok(block);
        }

        DataBlock::sort_with_type(block, &self.sort_desc, LimitType::None)
    }
}
