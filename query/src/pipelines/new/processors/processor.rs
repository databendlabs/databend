// Copyright 2022 Datafuse Labs.
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

use std::cell::UnsafeCell;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::BoxFuture;
use futures::FutureExt;
use petgraph::graph::node_index;
use petgraph::prelude::NodeIndex;

pub enum Event {
    NeedData,
    NeedConsume,
    Sync,
    Async,
    Finished,
}

// The design is inspired by ClickHouse processors
#[async_trait::async_trait]
pub trait Processor: Send {
    fn name(&self) -> &'static str;

    fn event(&mut self) -> Result<Event>;

    // Synchronous work.
    fn process(&mut self) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplemented process."))
    }

    // Asynchronous work.
    async fn async_process(&mut self) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplemented async_process."))
    }
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
    pub unsafe fn id(&self) -> NodeIndex {
        *self.id.get()
    }

    /// # Safety
    pub unsafe fn set_id(&self, id: NodeIndex) {
        *self.id.get() = id;
    }

    /// # Safety
    pub unsafe fn name(&self) -> &'static str {
        (*self.inner.get()).name()
    }

    /// # Safety
    pub unsafe fn event(&self) -> Result<Event> {
        (*self.inner.get()).event()
    }

    /// # Safety
    pub unsafe fn process(&self) -> Result<()> {
        (*self.inner.get()).process()
    }

    /// # Safety
    pub unsafe fn async_process(&self) -> BoxFuture<'static, Result<()>> {
        (*self.inner.get()).async_process().boxed()
    }
}

pub type Processors = Vec<ProcessorPtr>;
