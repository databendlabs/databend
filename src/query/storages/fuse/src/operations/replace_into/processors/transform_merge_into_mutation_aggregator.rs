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
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransformer;

use crate::operations::replace_into::meta::MergeIntoOperation;
use crate::operations::replace_into::mutator::MergeIntoOperationAggregator;

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for MergeIntoOperationAggregator {
    const NAME: &'static str = "MergeIntoMutationAggregator";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        // accumulate mutations
        let merge_into_operation = MergeIntoOperation::try_from(data)?;
        self.accumulate(merge_into_operation).await?;
        // no partial output
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        // apply mutations
        let mutation_logs = self.apply().await?;
        Ok(mutation_logs.map(|logs| logs.into()))
    }
}

impl MergeIntoOperationAggregator {
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
