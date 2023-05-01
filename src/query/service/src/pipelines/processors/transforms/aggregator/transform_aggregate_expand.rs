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

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::Value;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

pub struct TransformExpandGroupingSets {
    group_bys: Vec<(usize, DataType)>,
    grouping_ids: Vec<usize>,
}

impl TransformExpandGroupingSets {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        group_bys: Vec<(usize, DataType)>,
        grouping_ids: Vec<usize>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformExpandGroupingSets {
                grouping_ids,
                group_bys,
            },
        ))
    }
}

impl Transform for TransformExpandGroupingSets {
    const NAME: &'static str = "TransformExpandGroupingSets";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let num_rows = data.num_rows();
        let num_group_bys = self.group_bys.len();
        let mut output_blocks = Vec::with_capacity(self.grouping_ids.len());

        for &id in &self.grouping_ids {
            // Repeat data for each grouping set.
            let grouping_column = BlockEntry {
                data_type: DataType::Number(NumberDataType::UInt32),
                value: Value::Scalar(Scalar::Number(NumberScalar::UInt32(id as u32))),
            };
            let mut columns = data
                .columns()
                .iter()
                .cloned()
                .chain(vec![grouping_column])
                .collect::<Vec<_>>();
            let bits = !id;
            for i in 0..num_group_bys {
                let entry = unsafe {
                    let offset = self.group_bys.get_unchecked(i).0;
                    columns.get_unchecked_mut(offset)
                };
                if bits & (1 << i) == 0 {
                    // This column should be set to NULLs.
                    *entry = BlockEntry {
                        data_type: entry.data_type.wrap_nullable(),
                        value: Value::Scalar(Scalar::Null),
                    }
                } else {
                    *entry = BlockEntry {
                        data_type: entry.data_type.wrap_nullable(),
                        value: entry.value.clone().wrap_nullable(None),
                    }
                }
            }
            output_blocks.push(DataBlock::new(columns, num_rows));
        }

        DataBlock::concat(&output_blocks)
    }
}
