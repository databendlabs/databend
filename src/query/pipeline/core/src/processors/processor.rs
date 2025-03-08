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
use std::cell::UnsafeCell;
use std::ops::Deref;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use fastrace::prelude::*;
use futures::future::BoxFuture;
use futures::FutureExt;
use petgraph::graph::node_index;
use petgraph::prelude::NodeIndex;

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

    fn un_reacted(&self, _cause: EventCause, _id: usize) -> Result<()> {
        Ok(())
    }

    // When the synchronization task needs to run for a long time, the interrupt function needs to be implemented.
    fn interrupt(&self) {}

    // Synchronous work.
    fn process(&mut self) -> Result<()> {
        Err(ErrorCode::Unimplemented("Unimplemented process."))
    }

    // Asynchronous work.
    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        Err(ErrorCode::Unimplemented("Unimplemented async_process."))
    }

    fn prepare_spill_payload(&mut self) -> Result<bool> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented prepare_spill_payload",
        ))
    }

    async fn flush_spill_payload(&mut self) -> Result<bool> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented flush_spill_payload",
        ))
    }

    fn configure_peer_nodes(&mut self, nodes: &[String]) {
        // do nothing by default
    }

    fn details_status(&self) -> Option<String> {
        None
    }
}

// To keep ProcessPtr::async_process taking &self, instead of self,
// we need to wrap UnsafeCell<Box<(dyn Processor)>>, and make it Sync,
// so that later an Arc of it could be moved into the async closure,
// which async_process returns.
struct UnsafeSyncCelledProcessor(UnsafeCell<Box<(dyn Processor)>>);
unsafe impl Sync for UnsafeSyncCelledProcessor {}

impl Deref for UnsafeSyncCelledProcessor {
    type Target = UnsafeCell<Box<(dyn Processor)>>;

    fn deref(&self) -> &Self::Target {
        &(self.0)
    }
}

#[derive(Clone)]
pub struct ProcessorPtr {
    id: Arc<UnsafeCell<NodeIndex>>,
    inner: Arc<UnsafeSyncCelledProcessor>,
}

impl From<UnsafeCell<Box<(dyn Processor)>>> for UnsafeSyncCelledProcessor {
    fn from(value: UnsafeCell<Box<(dyn Processor)>>) -> Self {
        Self(value)
    }
}

unsafe impl Send for ProcessorPtr {}

unsafe impl Sync for ProcessorPtr {}

impl ProcessorPtr {
    pub fn create(inner: Box<dyn Processor>) -> ProcessorPtr {
        ProcessorPtr {
            id: Arc::new(UnsafeCell::new(node_index(0))),
            inner: Arc::new(UnsafeCell::new(inner).into()),
        }
    }

    /// # Safety
    pub unsafe fn as_any(&mut self) -> &mut dyn Any {
        (*self.inner.get()).as_any()
    }

    /// # Safety
    pub unsafe fn id(&self) -> NodeIndex {
        *self.id.get()
    }

    /// # Safety
    pub unsafe fn set_id(&self, id: NodeIndex) {
        *self.id.get() = id;
    }

    /// # Safety
    pub unsafe fn name(&self) -> String {
        (*self.inner.get()).name()
    }

    /// # Safety
    pub unsafe fn event(&self, cause: EventCause) -> Result<Event> {
        (*self.inner.get()).event_with_cause(cause)
    }

    /// # Safety
    pub unsafe fn un_reacted(&self, cause: EventCause) -> Result<()> {
        (*self.inner.get()).un_reacted(cause, self.id().index())
    }

    /// # Safety
    pub unsafe fn interrupt(&self) {
        (*self.inner.get()).interrupt()
    }

    /// # Safety
    pub unsafe fn process(&self) -> Result<()> {
        let mut name = self.name();
        name.push_str("::process");
        let _span = LocalSpan::enter_with_local_parent(name)
            .with_property(|| ("graph-node-id", self.id().index().to_string()));

        (*self.inner.get()).process()
    }

    /// # Safety
    pub unsafe fn async_process(&self) -> BoxFuture<'static, Result<()>> {
        let id = self.id();
        let mut name = self.name();
        name.push_str("::async_process");

        let task = (*self.inner.get()).async_process();

        // The `task` may have reference to the `Processor` that hold in `self.inner`,
        // so we need to move a clone of `self.inner` into the following async closure to keep the
        // `Processor` from being dropped before `task` is done.

        // e.g.
        // There may be scenarios where the 'ExecutingGraph' has already been dropped,
        // but the async task returned by async_process is still running; in this case,
        // there could be illegal memory access.

        let inner = self.inner.clone();
        async move {
            let span = Span::enter_with_local_parent(name)
                .with_property(|| ("graph-node-id", id.index().to_string()));
            task.in_span(span).await?;

            drop(inner);
            Ok(())
        }
        .boxed()
    }

    pub fn configure_peer_nodes(&self, nodes: &[String]) {
        unsafe { (*self.inner.get()).configure_peer_nodes(nodes) }
    }

    /// # Safety
    pub unsafe fn details_status(&self) -> Option<String> {
        (*self.inner.get()).details_status()
    }
}
