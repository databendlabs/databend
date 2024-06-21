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

use std::collections::HashSet;

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Value;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransformer;
use itertools::Itertools;
use log::info;

pub struct DeduplicateRowNumber {
    unique_row_number: HashSet<u64>,
    accepted_data: bool,
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for DeduplicateRowNumber {
    const NAME: &'static str = "DeduplicateRowNumber";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        self.accumulate(data).await?;
        // no partial output
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        if self.unique_row_number.is_empty() {
            Ok(Some(DataBlock::empty()))
        } else {
            self.apply().await
        }
    }
}

impl DeduplicateRowNumber {
    #[async_backtrace::framed]
    pub async fn accumulate(&mut self, data_block: DataBlock) -> Result<()> {
        // warning!!!: if all source data is matched, will
        // we receive a empty block as expected? the answer is yes.
        // but if there is still also some data unmatched, we won't receive
        // an empty block.
        if data_block.is_empty() {
            merge_into_distributed_empty_row_number(1);
            self.unique_row_number.clear();
            self.accepted_data = true;
            return Ok(());
        }

        let row_number_vec = get_row_number(&data_block, 0);
        merge_into_distributed_deduplicate_row_number(data_block.num_rows() as u32);
        if !self.accepted_data {
            self.unique_row_number = row_number_vec.into_iter().collect();
            merge_into_distributed_init_unique_number(self.unique_row_number.len() as u32);
            info!(
                "init unique_row_number_len:{}",
                self.unique_row_number.len(),
            );
            self.accepted_data = true;
            return Ok(());
        }

        let mut new_set = HashSet::with_capacity(self.unique_row_number.len());
        for number in row_number_vec {
            if self.unique_row_number.contains(&number) {
                new_set.insert(number);
            }
        }
        merge_into_distributed_new_set_len(new_set.len() as u32);
        info!("init new_set_len:{}", new_set.len());
        self.unique_row_number = new_set;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn apply(&mut self) -> Result<Option<DataBlock>> {
        let row_number_vecs = self.unique_row_number.clone().into_iter().collect_vec();
        merge_into_distributed_apply_row_number(row_number_vecs.len() as u32);
        Ok(Some(DataBlock::new_from_columns(vec![
            UInt64Type::from_data(row_number_vecs),
        ])))
    }
}

pub(crate) fn get_row_number(data_block: &DataBlock, row_number_idx: usize) -> Buffer<u64> {
    let row_number_col = data_block.get_by_offset(row_number_idx);
    assert_eq!(
        row_number_col.data_type,
        DataType::Number(NumberDataType::UInt64)
    );
    let value = row_number_col.value.try_downcast::<UInt64Type>().unwrap();
    match value {
        Value::Scalar(scalar) => Buffer::from(vec![scalar]),
        Value::Column(column) => column,
    }
}

impl DeduplicateRowNumber {
    pub fn create() -> Result<Self> {
        Ok(Self {
            unique_row_number: HashSet::new(),
            accepted_data: false,
        })
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        let processor_ptr =
            AsyncAccumulatingTransformer::create(input.clone(), output.clone(), self);
        PipeItem::create(ProcessorPtr::create(processor_ptr), vec![input], vec![
            output,
        ])
    }
}
