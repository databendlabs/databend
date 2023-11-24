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
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::BoxFuture;
use futures::FutureExt;
use minitrace::prelude::*;
use petgraph::graph::node_index;
use petgraph::prelude::NodeIndex;

use crate::processors::profile::Profile;

#[derive(Debug)]
pub enum Event {
    NeedData,
    NeedConsume,
    Sync,
    Async,
    Finished,
}

#[derive(Clone)]
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

    fn record_profile(&self, _profile: &Profile) {}
}

#[derive(Clone)]
pub struct ProcessorPtr {
    id: Arc<UnsafeCell<NodeIndex>>,
    inner: Arc<UnsafeCell<Box<dyn Processor>>>,
}

unsafe impl Send for ProcessorPtr {}

unsafe impl Sync for ProcessorPtr {}

impl ProcessorPtr {
    pub fn create(inner: Box<dyn Processor>) -> ProcessorPtr {
        ProcessorPtr {
            id: Arc::new(UnsafeCell::new(node_index(0))),
            inner: Arc::new(UnsafeCell::new(inner)),
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
    pub unsafe fn record_profile(&self, profile: &Profile) {
        (*self.inner.get()).record_profile(profile)
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

        async move {
            let span = Span::enter_with_local_parent(name)
                .with_property(|| ("graph-node-id", id.index().to_string()));

            task.in_span(span).await
        }
        .boxed()
    }
}

#[async_trait::async_trait]
impl<T: Processor + ?Sized> Processor for Box<T> {
    fn name(&self) -> String {
        (**self).name()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        (**self).as_any()
    }

    fn event(&mut self) -> Result<Event> {
        (**self).event()
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        (**self).event_with_cause(cause)
    }

    fn interrupt(&self) {
        (**self).interrupt()
    }

    fn process(&mut self) -> Result<()> {
        (**self).process()
    }

    async fn async_process(&mut self) -> Result<()> {
        (**self).async_process().await
    }
}
