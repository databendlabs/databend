// Copyright 2022 Datafuse Labs.
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

use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodSerializer;
use common_exception::Result;
use common_planners::Expression;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;

pub struct TransformLimitBy {
    limit: usize,
    limit_by_columns_name: Vec<String>,
    keys_count: HashMap<Vec<u8>, usize>,
}

impl TransformLimitBy {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        limit: usize,
        limit_by_exprs: &[Expression],
    ) -> Result<ProcessorPtr> {
        let limit_by_columns_name = limit_by_exprs.iter().map(|col| col.column_name()).collect();

        Ok(Transformer::create(input, output, TransformLimitBy {
            limit,
            limit_by_columns_name,
            keys_count: HashMap::new(),
        }))
    }
}

#[async_trait::async_trait]
impl Transform for TransformLimitBy {
    const NAME: &'static str = "LimitByTransform";

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        let mut filter = MutableBitmap::from_len_zeroed(block.num_rows());
        let method = HashMethodSerializer::default();
        let group_indices = method.group_by_get_indices(&block, &self.limit_by_columns_name)?;

        for (limit_by_key, (rows, _)) in group_indices {
            let count = self.keys_count.entry(limit_by_key.clone()).or_default();
            // limit reached, no need to check rows
            if *count >= self.limit {
                continue;
            }

            // limit not reached yet, still have room for rows, keep filling with row index
            let remaining = self.limit - *count;
            for row in rows.iter().take(std::cmp::min(remaining, rows.len())) {
                filter.set(*row as usize, *count <= self.limit);
                *count += 1;
            }
        }

        let array = BooleanArray::from_data(ArrowType::Boolean, filter.into(), None);
        let batch = block.try_into()?;
        let batch = arrow::compute::filter::filter_record_batch(&batch, &array)?;
        batch.try_into()
    }
}
