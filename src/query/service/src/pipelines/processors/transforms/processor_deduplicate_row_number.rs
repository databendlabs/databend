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

use common_exception::Result;
use common_expression::types::UInt64Type;
use common_expression::DataBlock;
use common_expression::FromData;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransform;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransformer;
use itertools::Itertools;

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
            self.unique_row_number.clear();
            self.accepted_data = true;
            return Ok(());
        }

        let row_number_vec = get_row_number(&data_block, 0)?;
        let row_number_set: HashSet<u64> = row_number_vec.iter().cloned().collect();
        assert_eq!(row_number_set.len(), row_number_vec.len());

        if !self.accepted_data {
            self.unique_row_number = row_number_set;
            self.accepted_data = true;
            return Ok(());
        }

        self.unique_row_number = self
            .unique_row_number
            .intersection(&row_number_set)
            .cloned()
            .collect();
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn apply(&mut self) -> Result<Option<DataBlock>> {
        let row_number_vecs = self.unique_row_number.clone().into_iter().collect_vec();
        Ok(Some(DataBlock::new_from_columns(vec![
            UInt64Type::from_data(row_number_vecs),
        ])))
    }
}

pub(crate) fn get_row_number(data_block: &DataBlock, row_number_idx: usize) -> Result<Vec<u64>> {
    let row_number_col = data_block.get_by_offset(row_number_idx);
    let value = row_number_col.value.try_downcast::<UInt64Type>().unwrap();
    match value {
        common_expression::Value::Scalar(scalar) => Ok(vec![scalar]),
        common_expression::Value::Column(column) => Ok(column.into_iter().collect_vec()),
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
