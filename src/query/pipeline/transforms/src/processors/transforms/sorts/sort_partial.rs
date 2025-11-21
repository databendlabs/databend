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
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

use crate::Transform;
use crate::Transformer;

pub struct TransformSortPartial {
    limit: LimitType,
    sort_columns_descriptions: Arc<[SortColumnDescription]>,
}

impl TransformSortPartial {
    pub fn new(limit: LimitType, sort_columns_descriptions: Arc<[SortColumnDescription]>) -> Self {
        Self {
            limit,
            sort_columns_descriptions,
        }
    }

    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        limit: LimitType,
        sort_columns_descriptions: Arc<[SortColumnDescription]>,
    ) -> Result<Box<dyn Processor>> {
        Ok(Transformer::create(input, output, TransformSortPartial {
            limit,
            sort_columns_descriptions,
        }))
    }
}

#[async_trait::async_trait]
impl Transform for TransformSortPartial {
    const NAME: &'static str = "SortPartialTransform";

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        DataBlock::sort_with_type(&block, &self.sort_columns_descriptions, self.limit)
    }
}
