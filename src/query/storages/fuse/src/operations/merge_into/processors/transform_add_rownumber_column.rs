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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Value;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;

const PREFIX_OFFSET: usize = 48;

pub struct TransformAddRowNumberColumnProcessor {
    // node_id
    prefix: u64,
    // current row number, row_number shouldn't be overflow 48bits
    row_number: Arc<AtomicU64>,
}

impl TransformAddRowNumberColumnProcessor {
    pub fn new(node_id: u16, row_number: Arc<AtomicU64>) -> Self {
        TransformAddRowNumberColumnProcessor {
            prefix: (node_id as u64) << PREFIX_OFFSET,
            row_number,
        }
    }
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        node_id: u16,
        row_number: Arc<AtomicU64>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformAddRowNumberColumnProcessor {
                prefix: (node_id as u64) << PREFIX_OFFSET,
                row_number,
            },
        )))
    }
}

impl TransformAddRowNumberColumnProcessor {
    fn generate_row_number(&mut self, num_rows: u64) -> u64 {
        let row_number = self.row_number.fetch_add(num_rows, Ordering::SeqCst);
        self.prefix | row_number
    }
}

#[async_trait::async_trait]
impl Transform for TransformAddRowNumberColumnProcessor {
    const NAME: &'static str = "TransformAddRowNumberColumnProcessor";
    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let num_rows = data.num_rows() as u64;
        let row_number = self.generate_row_number(num_rows);
        let mut row_numbers = Vec::with_capacity(data.num_rows());
        for number in row_number..row_number + num_rows {
            row_numbers.push(number);
        }
        merge_into_distributed_generate_row_numbers(row_numbers.len() as u32);
        let mut data_block = data;
        let row_number_entry = BlockEntry::new(
            DataType::Number(NumberDataType::UInt64),
            Value::Column(UInt64Type::from_data(row_numbers)),
        );

        data_block.add_column(row_number_entry);
        Ok(data_block)
    }
}
