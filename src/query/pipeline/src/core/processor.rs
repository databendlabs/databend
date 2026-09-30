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

// Logs from this module will show up as "[PIPELINE-EXECUTOR] ...".
databend_common_tracing::register_module_tag!("[PIPELINE-EXECUTOR]");

use std::any::Any;

use databend_common_base::runtime::ThreadTracker;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use petgraph::prelude::NodeIndex;

/// Checks whether the currently executing processor has been interrupted.
///
/// The executor installs the processor's tracking payload while polling or processing it. Calls
/// made outside processor execution are treated as not interrupted.
pub fn check_interrupt() -> Result<()> {
    if ThreadTracker::is_interrupted() {
        return Err(ErrorCode::aborting());
    }
    Ok(())
}

#[derive(Debug)]
pub enum Event {
    NeedData,
    NeedConsume,
    Sync,
    Async,
    Finished,
}

#[derive(Clone, Debug)]
pub enum EventCause {
    Other,
    // Which input of the processor triggers the event
    Input(usize),
    // Which output of the processor triggers the event
    Output(usize),
}

// The design is inspired by ClickHouse processors
#[async_trait::async_trait]
pub trait Processor: Send {
    fn name(&self) -> String;

    /// Reference used for downcast.
    fn as_any(&mut self) -> &mut dyn Any;

    fn event(&mut self) -> Result<Event> {
        Err(ErrorCode::Unimplemented(format!(
            "event is unimplemented in {}",
            self.name()
        )))
    }

    fn event_with_cause(&mut self, _cause: EventCause) -> Result<Event> {
        self.event()
    }

    // Synchronous work.
    fn process(&mut self) -> Result<()> {
        Err(ErrorCode::Unimplemented("Unimplemented process."))
    }

    // Asynchronous work.
    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        Err(ErrorCode::Unimplemented("Unimplemented async_process."))
    }

    fn details_status(&self) -> Option<String> {
        None
    }

    /// Whether the executor may cancel an in-flight `async_process` once every output port of
    /// this processor has finished.
    ///
    /// Return `true` for processors whose `async_process` can wait on something that never
    /// arrives after downstream stops consuming (e.g. a channel receive). When enabled, the
    /// executor drops the pending `async_process` future as soon as all outputs are finished and
    /// then calls `event()` as usual, so the processor must tolerate being dropped at any await
    /// point and its `event()` must return `Finished` once all outputs are finished.
    ///
    /// Leave it `false` when `async_process` does work that must complete even after downstream
    /// finished, such as `on_finish` cleanup.
    ///
    /// Read once when the executor graph is built.
    fn cancel_async_on_outputs_finished(&self) -> bool {
        false
    }

    /// Called after the processor's NodeIndex is assigned during graph construction.
    /// Processors that need a `std::task::Waker` should obtain the `ExecutorWaker`
    /// during creation (via `pipeline.get_waker()`) and use it here with the assigned id.
    fn set_id(&mut self, _id: NodeIndex) {}
}

/// Owned handle to a processor while a pipeline is being built.
///
/// It is deliberately not `Clone`: every processor has exactly one owner. The executor takes the
/// processor out with [`ProcessorPtr::into_inner`] and moves it between its graph and its workers,
/// so the compiler checks that only one thread drives a processor at a time.
pub struct ProcessorPtr {
    inner: Box<dyn Processor>,
}

impl ProcessorPtr {
    pub fn create(inner: Box<dyn Processor>) -> ProcessorPtr {
        ProcessorPtr { inner }
    }

    pub fn as_any(&mut self) -> &mut dyn Any {
        self.inner.as_any()
    }

    pub fn name(&self) -> String {
        self.inner.name()
    }

    pub fn into_inner(self) -> Box<dyn Processor> {
        self.inner
    }
}
