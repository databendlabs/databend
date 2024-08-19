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

use std::any::Any;
use std::sync::Arc;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::EventCause;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;

#[async_trait::async_trait]
pub trait PrefetchAsyncSource: Send {
    const NAME: &'static str;
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    async fn generate(&mut self) -> Result<Option<DataBlock>>;
    fn is_full(&self, prefetched: &[DataBlock]) -> bool;

    fn un_reacted(&self) -> Result<()> {
        Ok(())
    }
}

// TODO: This can be refactored using proc macros
// TODO: Most of its current code is consistent with sync. We need refactor this with better async
// scheduling after supported expand processors. It will be implemented using a similar dynamic window.
pub struct PrefetchAsyncSourcer<T: 'static + PrefetchAsyncSource> {
    is_inner_finish: bool,

    inner: T,
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,
    generated_data: Vec<DataBlock>,
}

impl<T: 'static + PrefetchAsyncSource> PrefetchAsyncSourcer<T> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        inner: T,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(Self {
            inner,
            output,
            scan_progress,
            is_inner_finish: false,
            generated_data: vec![],
        })))
    }
}

#[async_trait::async_trait]
impl<T: 'static + PrefetchAsyncSource> Processor for PrefetchAsyncSourcer<T> {
    fn name(&self) -> String {
        T::NAME.to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.is_inner_finish && self.generated_data.is_empty() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if self.output.can_push() {
            if let Some(data_block) = self.generated_data.pop() {
                self.output.push_data(Ok(data_block));
            }
        }

        if self.is_inner_finish || self.inner.is_full(&self.generated_data) {
            Ok(Event::NeedConsume)
        } else {
            Ok(Event::Async)
        }
    }

    fn un_reacted(&self, cause: EventCause, _id: usize) -> Result<()> {
        if let EventCause::Output(_) = cause {
            self.inner.un_reacted()?;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.inner.generate().await? {
            None => self.is_inner_finish = true,
            Some(data_block) => {
                // Don't need to record the scan progress of `MaterializedCteSource`
                // Because it reads data from memory.
                if !data_block.is_empty() && self.name() != "MaterializedCteSource" {
                    let progress_values = ProgressValues {
                        rows: data_block.num_rows(),
                        bytes: data_block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);
                    Profile::record_usize_profile(
                        ProfileStatisticsName::ScanBytes,
                        data_block.memory_size(),
                    );
                }

                if !T::SKIP_EMPTY_DATA_BLOCK || !data_block.is_empty() {
                    self.generated_data.push(data_block)
                }
            }
        };

        Ok(())
    }
}
