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
use std::time::Duration;

use databend_common_base::base::tokio::time::timeout;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::IndexType;

pub struct TransformRuntimeFilterWait {
    ctx: Arc<dyn TableContext>,
    scan_id: IndexType,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    runtime_filter_ready: Vec<Arc<RuntimeFilterReady>>,
    wait_finished: bool,
}

impl TransformRuntimeFilterWait {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        scan_id: IndexType,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(TransformRuntimeFilterWait {
            ctx,
            scan_id,
            input,
            output,
            runtime_filter_ready: Vec::new(),
            wait_finished: false,
        }))
    }
}

#[async_trait::async_trait]
impl Processor for TransformRuntimeFilterWait {
    fn name(&self) -> String {
        String::from("TransformRuntimeFilterWait")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.wait_finished {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data) = self.input.pull_data() {
            self.output.push_data(data);
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if self.runtime_filter_ready.is_empty() {
            self.runtime_filter_ready = self.ctx.get_runtime_filter_ready(self.scan_id);
        }

        if self.runtime_filter_ready.is_empty() {
            log::info!(
                "RUNTIME-FILTER: scan_id={} no runtime filters found, skipping wait",
                self.scan_id
            );
            self.wait_finished = true;
            return Ok(());
        }

        log::info!(
            "RUNTIME-FILTER: scan_id={} waiting for {} runtime filters",
            self.scan_id,
            self.runtime_filter_ready.len()
        );

        let timeout_duration = Duration::from_secs(30);
        for runtime_filter_ready in &self.runtime_filter_ready {
            let mut rx = runtime_filter_ready.runtime_filter_watcher.subscribe();
            if (*rx.borrow()).is_some() {
                continue;
            }

            match timeout(timeout_duration, rx.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    return Err(ErrorCode::TokioError("watcher's sender is dropped"));
                }
                Err(_) => {
                    log::warn!(
                        "Runtime filter wait timeout after {:?} for scan_id: {}",
                        timeout_duration,
                        self.scan_id
                    );
                }
            }
        }

        self.runtime_filter_ready.clear();
        self.wait_finished = true;
        Ok(())
    }
}
