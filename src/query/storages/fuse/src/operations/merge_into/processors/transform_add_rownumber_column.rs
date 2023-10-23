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

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::UInt64Type;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::Value;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

const PREFIX_OFFSET: usize = 48;

pub struct TransformAddRowNumberColumnProcessor {
    // node_id
    prefix: u16,
    // current row number, row_number shouldn't be overflow 48bits
    row_number: Arc<AtomicU64>,
}

impl TransformAddRowNumberColumnProcessor {
    #[allow(dead_code)]
    fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        node_id: u16,
        row_number: Arc<AtomicU64>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformAddRowNumberColumnProcessor {
                prefix: node_id,
                row_number,
            },
        ))
    }

    fn create_row_number_column_transform_item(
        node_id: u16,
        row_number: Arc<AtomicU64>,
    ) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        PipeItem::create(
            TransformAddRowNumberColumnProcessor::create(
                input.clone(),
                output.clone(),
                node_id,
                row_number,
            ),
            vec![input],
            vec![output],
        )
    }

    pub fn into_pipe(node_id: u16, num_threads: usize) -> Pipe {
        let mut pipe_items = Vec::with_capacity(num_threads);
        let row_number = Arc::new(AtomicU64::new(0));
        for _ in 0..num_threads {
            let pipe_item =
                Self::create_row_number_column_transform_item(node_id, row_number.clone());
            pipe_items.push(pipe_item);
        }
        Pipe::create(num_threads, num_threads, pipe_items)
    }
}

impl TransformAddRowNumberColumnProcessor {
    fn generate_row_number(&mut self, num_rows: u64) -> u64 {
        let row_number = self.row_number.fetch_add(num_rows, Ordering::SeqCst);
        let mut prefix_u64 = self.prefix as u64;
        prefix_u64 = prefix_u64 << PREFIX_OFFSET;
        prefix_u64 & row_number
    }
}

#[async_trait::async_trait]
impl Transform for TransformAddRowNumberColumnProcessor {
    const NAME: &'static str = "TransformAddRowNumberColumnProcessor";
    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let num_rows = data.num_rows() as u64;
        let last_row_number = self.generate_row_number(num_rows);
        let mut row_ids = Vec::with_capacity(data.num_rows());
        for number in (last_row_number - num_rows + 1)..=last_row_number {
            row_ids.push(number);
        }
        let mut data_block = data;
        let row_number_entry = BlockEntry::new(
            DataType::Number(NumberDataType::UInt64),
            Value::Column(UInt64Type::from_data(row_ids)),
        );
        data_block.add_column(row_number_entry);
        Ok(data_block)
    }
}
