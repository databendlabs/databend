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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransformer;
use databend_common_storages_fuse::operations::SourceFullMatched;
use log::info;

pub struct AccumulateRowNumber {
    data_blocks: Vec<DataBlock>,
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for AccumulateRowNumber {
    const NAME: &'static str = "AccumulateRowNumber";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        self.accumulate(data).await?;
        // no partial output
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        self.apply().await
    }
}

impl AccumulateRowNumber {
    #[async_backtrace::framed]
    pub async fn accumulate(&mut self, data_block: DataBlock) -> Result<()> {
        info!(
            "accept a block, num_rows:{:?},num_columns:{:?}",
            data_block.num_rows(),
            data_block.num_columns(),
        );
        // if matched all source data, we will get an empty block, but which
        // has source join schema,not only row_number,for compound_block project,
        // it will do nothing for empty block.
        if !data_block.is_empty() {
            assert_eq!(data_block.num_columns(), 1);
            assert_eq!(
                data_block.get_by_offset(0).data_type,
                DataType::Number(NumberDataType::UInt64)
            );
            self.data_blocks.push(data_block);
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn apply(&mut self) -> Result<Option<DataBlock>> {
        // for distributed execution, if it's insert-only
        // merge into , we use right anti join.if all source
        // data is matched, we can't get any block.
        if self.data_blocks.is_empty() {
            return Ok(Some(DataBlock::empty_with_meta(Box::new(
                SourceFullMatched,
            ))));
        }

        // row_numbers is small, so concat is ok.
        Ok(Some(DataBlock::concat(&self.data_blocks)?))
    }
}

impl AccumulateRowNumber {
    pub fn create() -> Result<Self> {
        Ok(Self {
            data_blocks: Vec::with_capacity(10),
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
